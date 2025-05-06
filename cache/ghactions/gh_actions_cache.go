package ghactions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	apiv1 "github.com/buchgr/bazel-remote/v2/cache/ghactions/results/api/v1"
	"github.com/twitchtv/twirp"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
)

var (
	_ backendproxy.Uploader = (*ghActionsCache)(nil)
	_ cache.Proxy           = (*ghActionsCache)(nil)
)

var ErrNoMatchingCacheEntry = errors.New("no matching cache entry found")

func New(
	baseUrl string,
	token string,
	accessLogger cache.Logger,
	errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int,
) cache.Proxy {
	httpClient := &http.Client{
		Transport: LoggingTransport(accessLogger),
	}

	hooks := &twirp.ClientHooks{
		RequestPrepared: func(ctx context.Context, req *http.Request) (context.Context, error) {
			if req.Header == nil {
				req.Header = make(http.Header)
			}

			req.Header["Authorization"] = []string{fmt.Sprintf("Bearer %s", token)}
			return ctx, nil
		},
	}

	actionsCache := &ghActionsCache{
		cacheClient: apiv1.NewCacheServiceJSONClient(
			strings.TrimSuffix(baseUrl, "/"),
			httpClient,
			twirp.WithClientHooks(hooks),
		),
		accessLogger: accessLogger,
		errorLogger:  errorLogger,
	}

	actionsCache.uploadQueue = backendproxy.StartUploaders(actionsCache, numUploaders, maxQueuedUploads)

	return actionsCache
}

type ghActionsCache struct {
	cacheClient  apiv1.CacheService
	uploadQueue  chan<- backendproxy.UploadReq
	accessLogger cache.Logger
	errorLogger  cache.Logger
}

func (g ghActionsCache) Put(_ context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if g.uploadQueue == nil {
		if closeErr := rc.Close(); closeErr != nil {
			g.errorLogger.Printf("failed to close file: %s", closeErr)
		}
		return
	}

	select {
	case g.uploadQueue <- backendproxy.UploadReq{
		Hash:        hash,
		LogicalSize: logicalSize,
		SizeOnDisk:  sizeOnDisk,
		Kind:        kind,
		Rc:          rc,
	}:
	default:
		g.errorLogger.Printf("too many uploads queued\n")
		_ = rc.Close()
	}
}

func (g ghActionsCache) Get(
	ctx context.Context,
	kind cache.EntryKind,
	hash string, _ int64,
) (rc io.ReadCloser, size int64, err error) {
	req := &apiv1.GetCacheEntryDownloadURLRequest{
		Key:     g.cacheKey(kind, hash),
		Version: hash,
	}

	urlResponse, err := g.cacheClient.GetCacheEntryDownloadURL(ctx, req)
	if err != nil {
		g.errorLogger.Printf("GHACTIONS - cache entry download URL: %v", err)
		return nil, -1, err
	}

	if !urlResponse.Ok {
		g.accessLogger.Printf("GHACTIONS - no entry for given kind and hash %s-%s, matched key %t", kind, hash, urlResponse.MatchedKey)
		return nil, -1, ErrNoMatchingCacheEntry
	}

	g.accessLogger.Printf("GHACTIONS - downloading cached file for %s-%s", kind, hash)
	blobClient, err := blockblob.NewClientWithNoCredential(urlResponse.SignedDownloadUrl, nil)
	if err != nil {
		g.errorLogger.Printf("GHACTIONS - download client for URL: %v", err)
		return nil, -1, err
	}

	downloadResp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		g.errorLogger.Printf("GHACTIONS - download stream: %v", err)
		return nil, -1, err
	}

	rc = downloadResp.Body

	if kind == cache.CAS {
		return casblob.ExtractLogicalSize(rc)
	}

	if downloadResp.ContentLength != nil {
		size = *downloadResp.ContentLength
	}

	return rc, size, nil
}

func (g ghActionsCache) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	req := &apiv1.GetCacheEntryDownloadURLRequest{
		Key:     g.cacheKey(kind, hash),
		Version: hash,
	}

	resp, err := g.cacheClient.GetCacheEntryDownloadURL(ctx, req)
	if err != nil {
		g.errorLogger.Printf("failed to get cache entry download url: %s", err)
		return false, -1
	}

	g.accessLogger.Printf("GHACTIONS - contains %s-%s: %t", kind, hash, resp.Ok)

	return resp.Ok, -1
}

func (g ghActionsCache) UploadFile(item backendproxy.UploadReq) {
	defer func() {
		_ = item.Rc.Close()
	}()

	var (
		uploadCtx = context.Background()
		cacheKey  = g.cacheKey(item.Kind, item.Hash)
	)

	req := &apiv1.CreateCacheEntryRequest{
		Key:     cacheKey,
		Version: item.Hash,
	}

	resp, err := g.cacheClient.CreateCacheEntry(uploadCtx, req)
	if err != nil {
		if twirpErr, ok := err.(twirp.Error); ok && twirpErr.Code() == twirp.AlreadyExists {
			g.accessLogger.Printf("GHACTIONS - cache entry with key %s already exists", cacheKey)
			return
		}

		g.errorLogger.Printf("GHACTIONS - failed to create cache entry, key %s: %s", cacheKey, err)
		return
	}

	uploadClient, err := blockblob.NewClientWithNoCredential(resp.SignedUploadUrl, nil)
	if err != nil {
		g.errorLogger.Printf("GHACTIONS - failed to create upload client: %s", err)
		return
	}

	_, err = uploadClient.UploadStream(uploadCtx, item.Rc, nil)
	if err != nil {
		g.errorLogger.Printf("GHACTIONS - failed to upload file: %s", err)
		return
	}

	finalizeResp, err := g.cacheClient.FinalizeCacheEntryUpload(uploadCtx, &apiv1.FinalizeCacheEntryUploadRequest{
		Key:       cacheKey,
		Version:   item.Hash,
		SizeBytes: item.LogicalSize,
	})
	if err != nil {
		g.errorLogger.Printf("GHACTIONS - failed to upload file: %s", err)
		return
	}

	if !finalizeResp.Ok {
		g.errorLogger.Printf("GHACTIONS - failed to finalize cache entry: %s", err)
		return
	}

	g.accessLogger.Printf("GHACTIONS - uploaded file %s-%s", item.Kind, item.Hash)
}

func (g ghActionsCache) cacheKey(kind cache.EntryKind, hash string) string {
	return fmt.Sprintf("%s-%s", kind.String(), hash)
}
