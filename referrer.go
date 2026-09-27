// Copyright the olareg contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package olareg

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"

	digest "github.com/sudo-bmitch/oci-digest"

	"github.com/olareg/olareg/types"
)

const (
	referrerFilterATParam       = "artifactType"
	referrerFilterATHeaderKey   = "OCI-Filters-Applied"
	referrerFilterATHeaderValue = "artifactType"
)

type referrerKey struct {
	dig          digest.Digest
	artifactType string
}

type referrerResponses [][]byte

// referrerGet searches for the referrers response in the index.
// All errors should return an empty response, no 404's should be generated.
func (s *Server) referrerGet(repoStr, arg string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filterAT := r.URL.Query().Get(referrerFilterATParam)
		cacheDig := r.URL.Query().Get("cache")
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 0 {
			page = 0
		}
		dig, err := digest.Parse(arg)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = types.ErrRespJSON(w, types.ErrInfoUnsupported("requested digest is not valid"))
			return
		}
		// most errors should return an empty index
		i := types.Index{
			SchemaVersion: 2,
			MediaType:     types.MediaTypeOCI1ManifestList,
			Manifests:     []types.Descriptor{},
		}
		repo, err := s.backend.RepoGet(repoStr)
		if err != nil {
			w.Header().Add("content-type", types.MediaTypeOCI1ManifestList)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(i)
			if !errors.Is(err, types.ErrRepoNotAllowed) {
				s.log.Info("failed to get repo", "err", err, "repo", repoStr, "arg", arg)
			}
			return
		}
		if cacheDig != "" && page != 0 {
			dig, err := digest.Parse(cacheDig)
			if err != nil {
				s.log.Info("paged referrers request for invalid cache digest", "cache", cacheDig, "repo", repoStr, "page", page)
				w.WriteHeader(http.StatusBadRequest)
				_ = types.ErrRespJSON(w, types.ErrInfoUnsupported("requested digest is not valid"))
				return
			}
			if cacheResp, err := s.referrerCache.Get(referrerKey{dig: dig, artifactType: filterAT}); err == nil && page < len(cacheResp) {
				if page+1 < len(cacheResp) {
					next := r.URL
					q := next.Query()
					q.Set("page", fmt.Sprintf("%d", page+1))
					next.RawQuery = q.Encode()
					w.Header().Add("Link", fmt.Sprintf("<%s>; rel=next", next.String()))
				}
				if filterAT != "" {
					w.Header().Add(referrerFilterATHeaderKey, referrerFilterATHeaderValue)
				}
				w.Header().Add("content-type", types.MediaTypeOCI1ManifestList)
				w.Header().Add("content-length", fmt.Sprintf("%d", len(cacheResp[page])))
				w.WriteHeader(http.StatusOK)
				_, err = w.Write(cacheResp[page]) //#nosec G705
				if err != nil {
					s.log.Info("failed to write referrers response", "err", err, "repo", repoStr, "arg", arg)
				}
				return
			}
			// cache search for paged data failed, regenerate from current state, only use page counter if digest matches
		}
		d, rl, err := repo.ReferrerList(dig)
		if err != nil {
			w.Header().Add("content-type", types.MediaTypeOCI1ManifestList)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(i)
			if !errors.Is(err, types.ErrNotFound) {
				s.log.Info("failed to list referrers", "err", err, "repo", repoStr, "arg", arg)
			}
			return
		}
		// check page cache for digest, two users requesting same referrer list
		if cacheResp, err := s.referrerCache.Get(referrerKey{dig: d.Digest, artifactType: filterAT}); err == nil {
			if page >= len(cacheResp) {
				page = 0
			}
			if page+1 < len(cacheResp) {
				next := r.URL
				q := next.Query()
				q.Set("cache", d.Digest.String())
				q.Set("page", fmt.Sprintf("%d", page+1))
				next.RawQuery = q.Encode()
				w.Header().Add("Link", fmt.Sprintf("<%s>; rel=next", next.String()))
			}
			if filterAT != "" {
				w.Header().Add(referrerFilterATHeaderKey, referrerFilterATHeaderValue)
			}
			w.Header().Add("content-type", types.MediaTypeOCI1ManifestList)
			w.Header().Add("content-length", fmt.Sprintf("%d", len(cacheResp[page])))
			w.WriteHeader(http.StatusOK)
			_, err = w.Write(cacheResp[page]) //#nosec G705
			if err != nil {
				s.log.Info("failed to write referrers response", "err", err, "repo", repoStr, "arg", arg)
			}
			return
		}
		// filter the result, paginate, and cache it
		if filterAT != "" {
			rl.Manifests = slices.DeleteFunc(rl.Manifests, func(cur types.Descriptor) bool { return cur.ArtifactType != filterAT })
			w.Header().Add(referrerFilterATHeaderKey, referrerFilterATHeaderValue)
		}
		split, err := referrerPaginate(rl, s.conf.API.Referrer.Limit)
		if err != nil {
			s.log.Info("failed splitting referrer list", "err", err, "repo", repoStr, "arg", arg, "digest", d.Digest.String())
		}
		if len(split) == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.referrerCache.Set(referrerKey{dig: d.Digest, artifactType: filterAT}, split)
		// set the requested page output and next link
		if page > 0 && (cacheDig != d.Digest.String() || page >= len(split)) {
			page = 0
		}
		if page+1 < len(split) {
			next := r.URL
			q := next.Query()
			q.Set("cache", d.Digest.String())
			q.Set("page", fmt.Sprintf("%d", page+1))
			next.RawQuery = q.Encode()
			w.Header().Add("Link", fmt.Sprintf("<%s>; rel=next", next.String()))
		}
		// write the requested page
		w.Header().Add("content-type", types.MediaTypeOCI1ManifestList)
		w.Header().Add("content-length", fmt.Sprintf("%d", len(split[page])))
		w.WriteHeader(http.StatusOK)
		//#nosec G705 this looks like a false positive
		_, err = w.Write(split[page])
		if err != nil {
			s.log.Info("failed to write referrers response", "err", err, "repo", repoStr, "arg", arg)
		}
	}
}

// referrerPaginate separates a referrer index into separate pages.
// This will fail if a single entry still exceeds the limit.
// Note this is designed for readability over efficiency.
func referrerPaginate(in types.Index, limit int64) ([][]byte, error) {
	if len(in.Manifests) == 0 {
		// the empty result must return a single page
		out, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		return [][]byte{out}, nil
	}
	result := [][]byte{}
	last := []byte{}
	cur := in.Copy()
	cur.Manifests = []types.Descriptor{}
	errs := []error{}
	for _, d := range in.Manifests {
		cur.Manifests = append(cur.Manifests, d)
		next, err := json.Marshal(cur)
		if err != nil {
			return nil, err
		}
		if int64(len(next)) > limit {
			if len(last) > 0 && int64(len(last)) <= limit {
				result = append(result, last)
			}
			cur.Manifests = []types.Descriptor{d}
			next, err = json.Marshal(cur)
			if err != nil {
				return nil, err
			}
			if int64(len(next)) > limit {
				errs = append(errs, fmt.Errorf("single descriptor greater than limit: %s", d.Digest))
				cur.Manifests = []types.Descriptor{}
				last = []byte{}
				continue
			}
		}
		last = next
	}
	if len(last) > 0 && int64(len(last)) <= limit {
		result = append(result, last)
	}
	if len(errs) > 0 {
		return result, errors.Join(errs...)
	}
	return result, nil
}

// // referrerAdd adds a new referrer entry to a given subject.
// func (s *Server) referrerAdd(repo store.Repo, subject digest.Digest, desc types.Descriptor) error {
// 	index, err := repo.IndexGet()
// 	if err != nil {
// 		return err
// 	}
// 	refResp := types.Index{
// 		SchemaVersion: 2,
// 		MediaType:     types.MediaTypeOCI1ManifestList,
// 	}
// 	// existing referrer response exists to update/replace, use that to populate index
// 	// all errors reading existing referrers result in defaulting to an initial empty response
// 	if dOld, err := index.GetByAnnotation(types.AnnotReferrerSubject, subject.String()); err == nil {
// 		func() {
// 			rdr, err := repo.BlobGet(dOld.Digest)
// 			if err != nil {
// 				return
// 			}
// 			err = json.NewDecoder(rdr).Decode(&refResp)
// 			_ = rdr.Close()
// 			if err != nil {
// 				return
// 			}
// 		}()
// 	}
// 	if mi := slices.IndexFunc(refResp.Manifests, func(cur types.Descriptor) bool { return desc.Digest.Equal(cur.Digest) }); mi >= 0 {
// 		// replace existing response with this digest
// 		refResp.Manifests[mi] = desc
// 	} else {
// 		// add descriptor to index
// 		refResp.Manifests = append(refResp.Manifests, desc)
// 	}
// 	// push the updated response to the blob store
// 	iRaw, err := json.Marshal(refResp)
// 	if err != nil {
// 		return err
// 	}
// 	dig, err := digest.Canonical.FromBytes(iRaw)
// 	if err != nil {
// 		return err
// 	}
// 	bc, _, err := repo.BlobCreate(store.BlobWithDigest(dig))
// 	if err != nil && !errors.Is(err, types.ErrBlobExists) {
// 		return err
// 	}
// 	if err == nil {
// 		_, err = bc.Write(iRaw)
// 		if err != nil {
// 			_ = bc.Close()
// 			return err
// 		}
// 		err = bc.Close()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// create new descriptor for referrers response to add into index.json
// 	dNew := types.Descriptor{
// 		MediaType: types.MediaTypeOCI1ManifestList,
// 		Size:      int64(len(iRaw)),
// 		Digest:    dig,
// 		Annotations: map[string]string{
// 			types.AnnotReferrerSubject: subject.String(),
// 		},
// 	}
// 	// adding the new response also deletes the previous response
// 	err = repo.IndexInsert(dNew, types.LayoutWithChildren(refResp.Manifests))
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// // referrerDelete removes a referrer entry from a subject.
// func (s *Server) referrerDelete(repo store.Repo, subject digest.Digest, desc types.Descriptor) error {
// 	// get the index.json
// 	index, err := repo.IndexGet()
// 	if err != nil {
// 		return err
// 	}
// 	// search for matching referrer descriptor
// 	dOld, err := index.GetByAnnotation(types.AnnotReferrerSubject, subject.String())
// 	if err != nil {
// 		if errors.Is(err, types.ErrNotFound) {
// 			return nil
// 		}
// 		return err
// 	}
// 	// read the old referrer response into an index
// 	rdr, err := repo.BlobGet(dOld.Digest)
// 	if err != nil {
// 		return err
// 	}
// 	refRespRaw, err := io.ReadAll(rdr)
// 	_ = rdr.Close()
// 	if err != nil {
// 		return err
// 	}
// 	refResp := types.Index{}
// 	err = json.Unmarshal(refRespRaw, &refResp)
// 	if err != nil {
// 		return err
// 	}
// 	// remove descriptor from response
// 	refResp.Manifests = slices.DeleteFunc(refResp.Manifests, func(cur types.Descriptor) bool { return desc.Digest.Equal(cur.Digest) })
// 	// push response back to blob store with a new digest
// 	refRespRaw, err = json.Marshal(refResp)
// 	if err != nil {
// 		return err
// 	}
// 	dig, err := digest.Canonical.FromBytes(refRespRaw)
// 	if err != nil {
// 		return err
// 	}
// 	bc, _, err := repo.BlobCreate(store.BlobWithDigest(dig))
// 	if err != nil && !errors.Is(err, types.ErrBlobExists) {
// 		return err
// 	}
// 	if err == nil {
// 		_, err = bc.Write(refRespRaw)
// 		if err != nil {
// 			_ = bc.Close()
// 			return err
// 		}
// 		err = bc.Close()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// create new descriptor for referrers response
// 	dNew := types.Descriptor{
// 		MediaType: types.MediaTypeOCI1ManifestList,
// 		Size:      int64(len(refRespRaw)),
// 		Digest:    dig,
// 		Annotations: map[string]string{
// 			types.AnnotReferrerSubject: subject.String(),
// 		},
// 	}
// 	// adding the new response also deletes the previous response
// 	err = repo.IndexInsert(dNew, types.LayoutWithChildren(refResp.Manifests))
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
