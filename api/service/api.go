package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/api"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	m "github.com/base-org/blob-archiver/api/metrics"
	"github.com/base-org/blob-archiver/common/storage"
	opmetrics "github.com/ethereum-optimism/optimism/op-service/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/log"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type httpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e httpError) write(w http.ResponseWriter) {
	w.WriteHeader(e.Code)
	_ = json.NewEncoder(w).Encode(e)
}

func (e httpError) Error() string {
	return e.Message
}

const (
	jsonAcceptType = "application/json"
	sszAcceptType  = "application/octet-stream"
	serverTimeout  = 60 * time.Second
)

var (
	errUnknownBlock = &httpError{
		Code:    http.StatusNotFound,
		Message: "Block not found",
	}
	errServerError = &httpError{
		Code:    http.StatusInternalServerError,
		Message: "Internal server error",
	}
)

func newBlockIdError(input string) *httpError {
	return &httpError{
		Code:    http.StatusBadRequest,
		Message: fmt.Sprintf("invalid block id: %s", input),
	}
}

func newIndicesError(input string) *httpError {
	return &httpError{
		Code:    http.StatusBadRequest,
		Message: fmt.Sprintf("invalid index input: %s", input),
	}
}

func newOutOfRangeError(input uint64, blobCount int) *httpError {
	return &httpError{
		Code:    http.StatusBadRequest,
		Message: fmt.Sprintf("invalid index: %d block contains %d blobs", input, blobCount),
	}
}

type API struct {
	dataStoreClient storage.DataStoreReader
	beaconClient    client.BeaconBlockHeadersProvider
	httpClient      *http.Client
	upstreamURL     *url.URL
	router          *chi.Mux
	logger          log.Logger
	metrics         m.Metricer
}

func NewAPI(dataStoreClient storage.DataStoreReader, beaconClient client.BeaconBlockHeadersProvider, upstreamURL *url.URL, metrics m.Metricer, logger log.Logger) *API {
	result := &API{
		dataStoreClient: dataStoreClient,
		beaconClient:    beaconClient,
		httpClient:      &http.Client{},
		upstreamURL:     upstreamURL,
		router:          chi.NewRouter(),
		logger:          logger,
		metrics:         metrics,
	}

	r := result.router
	r.Use(middleware.Logger)
	r.Use(middleware.Timeout(serverTimeout))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Heartbeat("/healthz"))
	r.Use(middleware.Compress(5, jsonAcceptType, sszAcceptType))

	recorder := opmetrics.NewPromHTTPRecorder(metrics.Registry(), m.MetricsNamespace)
	r.Use(func(handler http.Handler) http.Handler {
		return opmetrics.NewHTTPRecordingMiddleware(recorder, handler)
	})

	r.Get("/eth/v1/beacon/blob_sidecars/{id}", result.blobSidecarHandler)

	if upstreamURL != nil {
		r.NotFound(result.httpProxyHandler)
	}

	return result
}

func isHash(s string) bool {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") {
		return false
	}

	_, err := hexutil.Decode(s)
	return err == nil
}

func isSlot(id string) bool {
	_, err := strconv.ParseUint(id, 10, 64)
	return err == nil
}

func isKnownIdentifier(id string) bool {
	return slices.Contains([]string{"genesis", "finalized", "head"}, id)
}

// toBeaconBlockHash converts a string that can be a slot, hash or identifier to a beacon block hash.
func (a *API) toBeaconBlockHash(id string) (common.Hash, *httpError) {
	if isHash(id) {
		a.metrics.RecordBlockIdType(m.BlockIdTypeHash)
		return common.HexToHash(id), nil
	} else if isSlot(id) || isKnownIdentifier(id) {
		a.metrics.RecordBlockIdType(m.BlockIdTypeBeacon)
		result, err := a.beaconClient.BeaconBlockHeader(context.Background(), &api.BeaconBlockHeaderOpts{
			Common: api.CommonOpts{},
			Block:  id,
		})

		if err != nil {
			var apiErr *api.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
				return common.Hash{}, errUnknownBlock
			}

			return common.Hash{}, errServerError
		}

		return common.Hash(result.Data.Root), nil
	} else {
		a.metrics.RecordBlockIdType(m.BlockIdTypeInvalid)
		return common.Hash{}, newBlockIdError(id)
	}
}

// Hop-by-hop headers. These are removed when sent to the backend.
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te", // canonicalized version of "TE"
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func delHopHeaders(header http.Header) {
	for _, h := range hopHeaders {
		header.Del(h)
	}
}

func (a *API) httpProxyHandler(w http.ResponseWriter, r *http.Request) {
	a.logger.Info("proxying http request", "method", r.Method, "path", r.URL.Path)

	r.RequestURI = ""
	r.Host = ""

	delHopHeaders(r.Header)

	r.URL.Scheme = a.upstreamURL.Scheme
	r.URL.Host = a.upstreamURL.Host
	r.URL.User = a.upstreamURL.User
	r.URL.Path = path.Join(a.upstreamURL.Path, r.URL.Path)

	resp, err := a.httpClient.Do(r)
	if err != nil {
		http.Error(w, "Server Error", http.StatusInternalServerError)
		a.logger.Warn("http proxy upstream error", err)
	}
	defer resp.Body.Close()

	delHopHeaders(resp.Header)

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// blobSidecarHandler implements the /eth/v1/beacon/blob_sidecars/{id} endpoint, using the underlying DataStoreReader
// to fetch blobs instead of the beacon node. This allows clients to fetch expired blobs.
func (a *API) blobSidecarHandler(w http.ResponseWriter, r *http.Request) {
	param := chi.URLParam(r, "id")
	beaconBlockHash, err := a.toBeaconBlockHash(param)
	if err != nil {
		err.write(w)
		return
	}

	result, storageErr := a.dataStoreClient.Read(r.Context(), beaconBlockHash)
	if storageErr != nil {
		if errors.Is(storageErr, storage.ErrNotFound) {
			errUnknownBlock.write(w)
		} else {
			a.logger.Info("unexpected error fetching blobs", "err", storageErr, "beaconBlockHash", beaconBlockHash.String(), "param", param)
			errServerError.write(w)
		}
		return
	}

	blobSidecars := result.BlobSidecars

	filteredBlobSidecars, err := filterBlobs(blobSidecars.Data, r.URL.Query()["indices"])
	if err != nil {
		err.write(w)
		return
	}

	blobSidecars.Data = filteredBlobSidecars
	responseType := r.Header.Get("Accept")

	if responseType == sszAcceptType {
		w.Header().Set("Content-Type", sszAcceptType)
		res, err := blobSidecars.MarshalSSZ()
		if err != nil {
			a.logger.Error("unable to marshal blob sidecars to SSZ", "err", err)
			errServerError.write(w)
			return
		}

		_, err = w.Write(res)

		if err != nil {
			a.logger.Error("unable to write ssz response", "err", err)
			errServerError.write(w)
			return
		}
	} else {
		w.Header().Set("Content-Type", jsonAcceptType)
		err := json.NewEncoder(w).Encode(blobSidecars)
		if err != nil {
			a.logger.Error("unable to encode blob sidecars to JSON", "err", err)
			errServerError.write(w)
			return
		}
	}
}

// filterBlobs filters the blobs based on the indices query provided.
// If no indices are provided, all blobs are returned. If invalid indices are provided, an error is returned.
func filterBlobs(blobs []*deneb.BlobSidecar, _indices []string) ([]*deneb.BlobSidecar, *httpError) {
	var indices []string
	if len(_indices) == 0 {
		return blobs, nil
	} else if len(_indices) == 1 {
		indices = strings.Split(_indices[0], ",")
	} else {
		indices = _indices
	}

	indicesMap := map[deneb.BlobIndex]struct{}{}
	for _, index := range indices {
		parsedInt, err := strconv.ParseUint(index, 10, 64)
		if err != nil {
			return nil, newIndicesError(index)
		}

		if parsedInt >= uint64(len(blobs)) {
			return nil, newOutOfRangeError(parsedInt, len(blobs))
		}

		blobIndex := deneb.BlobIndex(parsedInt)
		indicesMap[blobIndex] = struct{}{}
	}

	filteredBlobs := make([]*deneb.BlobSidecar, 0)
	for _, blob := range blobs {
		if _, ok := indicesMap[blob.Index]; ok {
			filteredBlobs = append(filteredBlobs, blob)
		}
	}

	return filteredBlobs, nil
}
