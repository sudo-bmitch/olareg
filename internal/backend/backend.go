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

// Package backend creates an interface for OCI and registry abstractions on top of a [store.Store].
package backend

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/olareg/olareg/config"
	"github.com/olareg/olareg/internal/cache"
	"github.com/olareg/olareg/internal/reproducible"
	"github.com/olareg/olareg/internal/store"
	"github.com/olareg/olareg/types"
	digest "github.com/sudo-bmitch/oci-digest"
)

const (
	indexFile  = "index.json"
	layoutFile = "oci-layout"
	blobsDir   = "blobs"
)

// - Manifest delete: refuse internal content (referrers, history)
// - Tag history: config to trim old entries
// - GC: for each repo, write lock, clean stale uploads, walk index applying policy, update manifest descriptor cache, push updated index and call repo.GC with cutoff time and list of blobs to keep, close repo after enough time has passed

// Backend is used to manage a collection of OCI Layouts.
type Backend struct {
	mu    sync.Mutex
	conf  config.Config
	store store.Store
	repos map[string]*Repo // TODO: switch to a cache that runs a GC when pruned
	stop  chan struct{}
}

// Repo is used to manage a single OCI Layout.
type Repo struct {
	mu        sync.RWMutex
	conf      config.Config
	mod       time.Time
	store     store.Repo
	index     types.LayoutIndex
	tags      map[string]digest.Digest
	manifests map[digest.Digest]types.Descriptor
	referrers map[digest.Digest]indexCache
	history   map[string]indexCache
	uploads   *cache.Cache[string, *uploadSession]
}

type UploadStatus struct {
	Size int64
}

type indexCache struct {
	descriptor types.Descriptor
	index      types.Index
	raw        []byte
}

type uploadSession struct {
	mu        sync.Mutex
	bc        store.BlobCreator
	alg       digest.Algorithm
	w         digest.Writer
	size      int64
	sessionID string
}

type BlobOpt func(*blobConfig) error

type blobConfig struct {
	algo   digest.Algorithm
	expect digest.Digest
	b      []byte
	rdr    io.Reader
}

type manifestSubject struct {
	MediaType    string            `json:"mediaType,omitempty"`
	ArtifactType string            `json:"artifactType,omitempty"`
	Config       types.Descriptor  `json:"config"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Subject      *types.Descriptor `json:"subject,omitempty"`
}

// BlobWithAlgorithm configures the digest algorithm used while uploading the blob.
func BlobWithAlgorithm(a digest.Algorithm) BlobOpt {
	return func(bc *blobConfig) error {
		bc.algo = a
		return nil
	}
}

// BlobWithBytes immediately uploads the blob without creating an upload session.
func BlobWithBytes(b []byte) BlobOpt {
	return func(bc *blobConfig) error {
		bc.b = b
		return nil
	}
}

// BlobWithDigest is used to set the expected digest when creating the blob from a reader or bytes.
func BlobWithDigest(d digest.Digest) BlobOpt {
	return func(bc *blobConfig) error {
		if d.IsZero() {
			return fmt.Errorf("invalid digest: %s", d.String())
		}
		bc.expect = d
		bc.algo = d.Algorithm()
		return nil
	}
}

// BlobWithReader immediately uploads the blob without creating an upload session.
func BlobWithReader(rdr io.Reader) BlobOpt {
	return func(bc *blobConfig) error {
		bc.rdr = rdr
		return nil
	}
}

// New creates a new backend with the associated storage.
func New(conf config.Config) (*Backend, error) {
	s, err := store.New(conf.Storage)
	if err != nil {
		return nil, err
	}
	b := &Backend{
		conf:  conf,
		store: s,
		repos: map[string]*Repo{},
		stop:  make(chan struct{}),
	}
	go b.gcTick()
	return b, nil
}

// RepoGet loads a [Repo] from the underlying storage, or initializes an empty repo if it is new.
// If the underlying storage was not written by olareg, the repository may be scanned for nested manifests and to generate referrer responses.
func (b *Backend) RepoGet(path string) (*Repo, error) {
	// validate the path
	if !types.PathRE.MatchString(path) {
		return nil, fmt.Errorf("repo path is invalid: %s%.0w", path, types.ErrRepoNotAllowed)
	}
	for dir := range strings.SplitSeq(path, "/") {
		if slices.Contains([]string{indexFile, layoutFile, blobsDir}, dir) {
			return nil, fmt.Errorf("repo %s cannot contain %s, %s, or %s%.0w", path, indexFile, layoutFile, blobsDir, types.ErrRepoNotAllowed)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store == nil {
		return nil, fmt.Errorf("backend not initialized or was closed")
	}
	if r, ok := b.repos[path]; ok && r != nil {
		return r, nil
	}
	sr, err := b.store.RepoGet(path)
	if err != nil {
		return nil, err
	}
	ind, err := sr.IndexGet()
	if err != nil {
		return nil, err
	}
	r := &Repo{
		conf:      b.conf,
		store:     sr,
		index:     ind,
		tags:      map[string]digest.Digest{},
		manifests: map[digest.Digest]types.Descriptor{},
		referrers: map[digest.Digest]indexCache{},
		history:   map[string]indexCache{},
	}
	uploadCacheOpt := cache.Opts[string, *uploadSession]{
		PruneFn: func(s string, us *uploadSession) error {
			_ = us.bc.Cancel()
			return nil
		},
	}
	if b.conf.Storage.GC.RepoUploadMax > 0 {
		uploadCacheOpt.Count = b.conf.Storage.GC.RepoUploadMax
	}
	if b.conf.Storage.GC.GracePeriod > 0 {
		uploadCacheOpt.Age = b.conf.Storage.GC.GracePeriod
	}
	r.uploads = cache.New(uploadCacheOpt)
	b.repos[path] = r
	// load values from the index.json for easier access
	for _, desc := range r.index.Manifests {
		r.manifests[desc.Digest] = desc
		if tag, ok := desc.AnnotationVal(types.AnnotRefName); ok {
			r.tags[tag] = desc.Digest
		}
		if subj, ok := desc.AnnotationVal(types.AnnotReferrerSubject); ok {
			if subjDig, err := digest.Parse(subj); err == nil {
				r.referrers[subjDig] = indexCache{descriptor: desc}
			} else {
				// force a walk on a parsing failure
				r.index.OlaregExt.Referrers = false
			}
		}
		if tag, ok := desc.AnnotationVal(types.AnnotTagHistory); ok {
			r.history[tag] = indexCache{descriptor: desc}
		}
	}
	errList := []error{}
	indexChanged := false
	// if the index is not empty and missing either child manifests or referrers from olareg, walk it
	if len(r.index.Manifests) > 0 && !(r.index.OlaregExt.ChildManifests && r.index.OlaregExt.Referrers) {
		manifestsNew := map[digest.Digest]types.Descriptor{}
		referrersNew := map[digest.Digest][]types.Descriptor{}
		for step := range sr.Walk(types.ParseManifests, true) {
			manifestsNew[step.Desc.Digest] = step.Desc
			b, err := io.ReadAll(step.Rdr)
			_ = step.Rdr.Close()
			if err != nil {
				errList = append(errList, err)
				continue
			}
			// parse referrer responses into r.referrers cache
			if subj, ok := step.Desc.AnnotationVal(types.AnnotReferrerSubject); ok {
				dig, err := digest.Parse(subj)
				respInd := types.Index{}
				if err == nil {
					err = json.Unmarshal(b, &respInd)
				}
				if err == nil {
					r.referrers[dig] = indexCache{
						descriptor: step.Desc,
						index:      respInd,
						raw:        b,
					}
				} else {
					errList = append(errList, err)
				}
			}
			// parse history into r.history cache
			if tag, ok := step.Desc.AnnotationVal(types.AnnotTagHistory); ok {
				respInd := types.Index{}
				err = json.Unmarshal(b, &respInd)
				if err == nil {
					r.history[tag] = indexCache{
						descriptor: step.Desc,
						index:      respInd,
						raw:        b,
					}
				} else {
					errList = append(errList, err)
				}
			}
			if subj, resp, err := types.ManifestReferrerDescriptor(b, step.Desc); err == nil {
				if !slices.ContainsFunc(referrersNew[subj.Digest], func(cur types.Descriptor) bool { return cur.Digest.Equal(resp.Digest) }) {
					referrersNew[subj.Digest] = append(referrersNew[subj.Digest], resp)
				}
			} else if !errors.Is(err, types.ErrNotFound) {
				errList = append(errList, err)
			}
		}
		r.manifests = manifestsNew
		// delete missing manifests from index
		for i, v := range slices.Backward(r.index.Manifests) {
			if _, ok := manifestsNew[v.Digest]; !ok {
				r.index.Manifests = slices.Delete(r.index.Manifests, i, i+1)
				indexChanged = true
			}
		}
		// add missing manifests to index
		for dig, desc := range manifestsNew {
			if !slices.ContainsFunc(r.index.Manifests, func(cur types.Descriptor) bool { return cur.Digest.Equal(dig) }) {
				r.index.Manifests = append(r.index.Manifests, desc)
				indexChanged = true
			}
		}
		// update referrers responses
		for subj, respCache := range r.referrers {
			newList, ok := referrersNew[subj]
			if !ok {
				// prune stale referrers response
				if pos := slices.IndexFunc(r.index.Manifests, func(cur types.Descriptor) bool { return cur.Equal(respCache.descriptor) }); pos >= 0 {
					r.index.Manifests = slices.Delete(r.index.Manifests, pos, pos+1)
				}
				delete(r.referrers, subj)
				continue
			}
			changed := false
			// prune referrers that no longer exist
			startLen := len(respCache.index.Manifests)
			respCache.index.Manifests = slices.DeleteFunc(respCache.index.Manifests, func(cur types.Descriptor) bool {
				return !slices.ContainsFunc(newList, func(newEntry types.Descriptor) bool { return cur.Digest.Equal(newEntry.Digest) })
			})
			if startLen != len(respCache.index.Manifests) {
				changed = true
			}
			// add referrers that weren't listed
			for _, desc := range newList {
				if !slices.ContainsFunc(respCache.index.Manifests, func(cur types.Descriptor) bool { return cur.Digest.Equal(desc.Digest) }) {
					respCache.index.Manifests = append(respCache.index.Manifests, desc)
					changed = true
				}
			}
			// refresh response cache, optionally writing back to the underlying store
			if changed {
				err := r.cachePut(&respCache)
				if err != nil {
					errList = append(errList, err)
					continue
				}
				mod, err := r.indexAddByAnnotation(respCache.descriptor, types.AnnotReferrerSubject, subj.String())
				if err != nil {
					errList = append(errList, err)
					continue
				}
				if mod {
					indexChanged = true
				}
			} else {
				// trim cache, pull from store
				respCache.raw = nil
				respCache.index = types.Index{}
			}
			r.referrers[subj] = respCache
		}
		// add referrer responses that are missing from the index
		for subj, descList := range referrersNew {
			if _, ok := r.referrers[subj]; ok {
				continue
			}
			respCache := cacheNew()
			respCache.index.Manifests = descList
			err := r.cachePut(&respCache)
			if err != nil {
				errList = append(errList, err)
				continue
			}
			r.referrers[subj] = respCache
			mod, err := r.indexAddByAnnotation(respCache.descriptor, types.AnnotReferrerSubject, subj.String())
			if err != nil {
				errList = append(errList, err)
				continue
			}
			if mod {
				indexChanged = true
			}
		}
		r.index.OlaregExt.ChildManifests = true
		r.index.OlaregExt.Referrers = true
	}
	// TODO: initialize history if missing
	if indexChanged && !*b.conf.Storage.ReadOnly {
		err := r.store.IndexSet(r.index)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Close frees resources used by the backend.
// No further calls should be made to the backend after this.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	errs := []error{}
	for name, repo := range b.repos {
		if err := repo.uploads.DeleteAll(); err != nil {
			errs = append(errs, fmt.Errorf("failed to delete uploads from repo %s: %w", name, err))
		}
		if err := repo.store.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close repo %s: %w", name, err))
		}
	}
	b.repos = nil
	if err := b.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close store: %w", err))
	}
	b.store = nil
	close(b.stop)
	return errors.Join(errs...)
}

// gcTick is the goroutine to periodically prune all recently modified repos.
func (b *Backend) gcTick() {
	if b.conf.Storage.GC.Frequency <= 0 {
		return
	}
	ticker := time.NewTicker(b.conf.Storage.GC.Frequency)
	prev := time.Time{}
	for {
		select {
		case cur := <-ticker.C:
			b.gc(cur, prev)
			prev = cur
		case <-b.stop:
			ticker.Stop()
			return
		}
	}
}

func (b *Backend) gc(cur, prev time.Time) {
	start := prev
	if b.conf.Storage.GC.GracePeriod > 0 {
		start = start.Add(b.conf.Storage.GC.GracePeriod * -1)
	}
	// only hold the lock for long enough to get all the repo pointers
	b.mu.Lock()
	repos := make([]*Repo, 0, len(b.repos))
	for _, r := range b.repos {
		repos = append(repos, r)
	}
	b.mu.Unlock()
	for _, repo := range repos {
		// if stop ch was closed, exit immediately
		select {
		case <-b.stop:
			return
		default:
		}
		// skip repos that were not updated since the last check, offsetting for the grace period
		repo.mu.RLock()
		outsideRange := repo.mod.Before(start)
		repo.mu.RUnlock()
		if outsideRange {
			continue
		}
		repo.gc(cur)
	}
}

// BlobDelete removes the requested blob.
func (r *Repo) BlobDelete(dig digest.Digest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.manifests[dig]; ok {
		return fmt.Errorf("cannot delete a manifest from the blob API%.0w", types.ErrForbidden)
	}
	r.mod = time.Now()
	return r.store.BlobDelete(dig)
}

// BlobGet returns the request blob.
func (r *Repo) BlobGet(dig digest.Digest) (io.ReadSeekCloser, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rdr, err := r.store.BlobGet(dig)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the blob %s: %w", dig.String(), err)
	}
	return rdr, nil
}

// ManifestDelete removes the requested manifest by digest.
func (r *Repo) ManifestDelete(dig digest.Digest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	desc, ok := r.manifests[dig]
	if !ok {
		return fmt.Errorf("manifest not found: %s%.0w", dig.String(), types.ErrNotFound)
	}
	r.mod = time.Now()
	// remove referrers responses to the manifest
	modRef, err := r.referrersDel(dig)
	if err != nil {
		return err
	}
	// remove from the blob store, ignoring "not found" errors
	err = r.store.BlobDelete(dig)
	if err != nil && !errors.Is(err, types.ErrNotFound) {
		return err
	}
	// remove from the index
	modIndex, err := r.indexDelByDigest(desc)
	if err != nil {
		return err
	}
	// remove from the descriptor map
	delete(r.manifests, dig)
	// write index
	if !modRef && !modIndex {
		return nil
	}
	err = r.store.IndexSet(r.index)
	if err != nil {
		return err
	}
	return nil
}

// ManifestGet returns the a manifest by digest.
func (r *Repo) ManifestGet(dig digest.Digest) (types.Descriptor, io.ReadSeekCloser, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	desc, ok := r.manifests[dig]
	if !ok {
		return types.Descriptor{}, nil, fmt.Errorf("manifest %s not found%.0w", dig.String(), types.ErrNotFound)
	}
	rdr, err := r.store.BlobGet(dig)
	if err != nil {
		return types.Descriptor{}, nil, fmt.Errorf("failed to retrieve the manifest %s: %.0w", dig.String(), err)
	}
	return desc, rdr, nil
}

// ManifestHead returns the descriptor for a manifest.
func (r *Repo) ManifestHead(dig digest.Digest) (types.Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	desc, ok := r.manifests[dig]
	if !ok {
		return types.Descriptor{}, fmt.Errorf("manifest %s not found%.0w", dig.String(), types.ErrNotFound)
	}
	return desc, nil
}

// ManifestPut pushes a new manifest to the store after validation.
// Tag history and referrers are also managed.
func (r *Repo) ManifestPut(b []byte, mediaType string, dig digest.Digest, tags []string) (types.Descriptor, []string, digest.Digest, error) {
	// validate the media type
	if mediaType == "" {
		mediaType = types.MediaTypeDetect(b)
	}
	if !types.MediaTypeManifest(mediaType) {
		return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("unsupported media type %s%.0w", mediaType, types.ErrManifestInvalid)
	}
	// validate (or compute) the digest
	if dig.IsZero() {
		var err error
		dig, err = digest.Canonical.FromBytes(b)
		if err != nil {
			return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("failed to compute digest: %w", err)
		}
	} else {
		comp, err := dig.Algorithm().FromBytes(b)
		if err != nil {
			return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("failed to compute digest: %w", err)
		}
		if !comp.Equal(dig) {
			return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("digest mismatch, expected %s, computed %s%.0w", dig.String(), comp.String(), types.ErrDigestInvalid)
		}
	}
	// validate the tags
	for _, tag := range tags {
		if !types.RefTagRE.MatchString(tag) {
			return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("invalid tag value: %s%.0w", tag, types.ErrUnsupported)
		}
	}
	// validate the manifest body (media type and descriptors)
	if err := r.manifestVerify(b, mediaType); err != nil {
		return types.Descriptor{}, nil, digest.Digest{}, err
	}
	// push to blob store
	bc, err := r.store.BlobCreate()
	if err != nil {
		return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("failed to create manifest in blob store: %w", err)
	}
	_, err = bc.Write(b)
	if err != nil {
		_ = bc.Cancel()
		return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("failed to write manifest to blob store: %w", err)
	}
	err = bc.Save(dig)
	if err != nil {
		return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("failed to save manifest to blob store: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mod = time.Now()
	// add entry to manifests
	desc := types.Descriptor{
		MediaType: mediaType,
		Digest:    dig,
		Size:      int64(len(b)),
	}
	r.manifests[dig] = desc
	// update index and manage tags
	modIndex := false
	if len(tags) == 0 {
		modIndex, err = r.indexAddByDigest(desc)
		if err != nil {
			return types.Descriptor{}, nil, digest.Digest{}, fmt.Errorf("failed to update the index: %w", err)
		}
	}
	procTags := []string{}
	for _, tag := range tags {
		mod, err := r.indexAddByAnnotation(desc, types.AnnotRefName, tag)
		if err != nil {
			continue
		}
		procTags = append(procTags, tag)
		modIndex = modIndex || mod
	}
	subjDig, modIndexRef, err := r.referrersAdd(b, mediaType, dig)
	if err != nil {
		return types.Descriptor{}, nil, digest.Digest{}, err
	}
	if modIndex || modIndexRef {
		err = r.store.IndexSet(r.index)
		if err != nil {
			return types.Descriptor{}, nil, digest.Digest{}, err
		}
	}
	return desc, procTags, subjDig, nil
}

func (r *Repo) manifestVerify(b []byte, mt string) error {
	// TODO: limit URL values according to the config
	switch mt {
	case types.MediaTypeDocker2Manifest, types.MediaTypeOCI1Manifest:
		m := types.Manifest{}
		err := json.Unmarshal(b, &m)
		if err != nil {
			return fmt.Errorf("failed to parse manifest: %w%.0w", err, types.ErrManifestInvalid)
		}
		if m.MediaType != "" && m.MediaType != mt {
			return fmt.Errorf("media type mismatch, expected %s, received %s%.0w", mt, m.MediaType, types.ErrManifestInvalid)
		}
		if !*r.conf.API.Manifest.SparseIndex {
			if _, err := r.store.BlobMeta(m.Config.Digest); err != nil {
				return fmt.Errorf("blob %s not found%.0w", m.Config.Digest.String(), types.ErrManifestBlobUnknown)
			}
			for _, d := range m.Layers {
				if len(d.URLs) > 0 || types.MediaTypeForeign(d.MediaType) {
					continue
				}
				if _, err := r.store.BlobMeta(d.Digest); err != nil {
					return fmt.Errorf("blob %s not found%.0w", d.Digest.String(), types.ErrManifestBlobUnknown)
				}
			}
		}
	case types.MediaTypeDocker2ManifestList, types.MediaTypeOCI1ManifestList:
		m := types.Index{}
		err := json.Unmarshal(b, &m)
		if err != nil {
			return fmt.Errorf("failed to parse manifest: %w%.0w", err, types.ErrManifestInvalid)
		}
		if m.MediaType != "" && m.MediaType != mt {
			return fmt.Errorf("media type mismatch, expected %s, received %s%.0w", mt, m.MediaType, types.ErrManifestInvalid)
		}
		if !*r.conf.API.Manifest.SparseIndex {
			for _, d := range m.Manifests {
				if !types.MediaTypeManifest(d.MediaType) {
					return fmt.Errorf("unsupported manifest media type: %s%.0w", d.MediaType, types.ErrManifestBlobUnknown)
				}
				if _, ok := r.manifests[d.Digest]; !ok {
					return fmt.Errorf("manifest %s not found%.0w", d.Digest.String(), types.ErrManifestBlobUnknown)
				}
			}
		}
	default:
		return fmt.Errorf("unsupported manifest media type: %s%.0w", mt, types.ErrManifestInvalid)
	}
	return nil
}

// ReferrerList returns the list of referrers for a given subject.
func (r *Repo) ReferrerList(subj digest.Digest) (types.Descriptor, types.Index, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ic, ok := r.referrers[subj]
	if !ok {
		return types.Descriptor{}, types.Index{}, types.ErrNotFound
	}
	err := r.cacheGet(&ic)
	if err != nil {
		return types.Descriptor{}, types.Index{}, err
	}
	return ic.descriptor, ic.index.Copy(), nil
}

// TagRm deletes a tag.
// The untagged manifest will remain referenced in the index.json.
func (r *Repo) TagDelete(tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	mod, err := r.indexDelByAnnotation(types.AnnotRefName, tag)
	if err != nil {
		return err
	}
	if !mod {
		return nil
	}
	// update the index
	r.mod = time.Now()
	return r.store.IndexSet(r.index)
}

// TagGet returns the digest for a specific tag.
func (r *Repo) TagGet(tag string) (digest.Digest, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dig, ok := r.tags[tag]
	if !ok {
		return digest.Digest{}, fmt.Errorf("tag %s not found%.0w", tag, types.ErrNotFound)
	}
	return dig, nil
}

// TagList returns a list of tags and their descriptors.
func (r *Repo) TagList() (map[string]types.Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tags := map[string]types.Descriptor{}
	for _, desc := range r.index.Manifests {
		if t, ok := desc.AnnotationVal(types.AnnotRefName); ok {
			tags[t] = desc.Copy()
		}
	}
	return tags, nil
}

// UploadCancel aborts a running upload.
func (r *Repo) UploadCancel(sessionID string) error {
	us, err := r.uploads.Get(sessionID)
	if err != nil {
		return fmt.Errorf("session %s: %w", sessionID, err)
	}
	err = us.bc.Cancel()
	if err != nil {
		return err
	}
	return r.uploads.Delete(sessionID)
}

// UploadCreate begins a new upload.
func (r *Repo) UploadCreate(opts ...BlobOpt) (string, error) {
	conf := blobConfig{
		algo: digest.Canonical,
	}
	for _, opt := range opts {
		err := opt(&conf)
		if err != nil {
			return "", err
		}
	}
	// verify blob does not already exist
	if !conf.expect.IsZero() {
		if _, err := r.store.BlobMeta(conf.expect); err == nil {
			return "", types.ErrBlobExists
		}
	}
	// if bytes were passed, create blob directly
	if conf.b != nil {
		d, err := conf.algo.FromBytes(conf.b)
		if err != nil {
			return "", err
		}
		if !conf.expect.IsZero() && !conf.expect.Equal(d) {
			return "", fmt.Errorf("digest mismatch, expected %s, received %s", conf.expect.String(), d.String())
		}
		bc, err := r.store.BlobCreate()
		if err != nil {
			return "", err
		}
		_, err = bc.Write(conf.b)
		if err != nil {
			_ = bc.Cancel()
			return "", err
		}
		err = bc.Save(d)
		if err != nil {
			return "", err
		}
		r.mod = time.Now()
		return "", nil
	}
	// if a reader was passed, create the blob directly
	if conf.rdr != nil {
		bc, err := r.store.BlobCreate()
		if err != nil {
			return "", err
		}
		w := digest.NewWriter(bc, conf.algo)
		_, err = io.Copy(w, conf.rdr)
		if err != nil {
			_ = bc.Cancel()
			return "", err
		}
		d, err := w.Digest()
		if err != nil {
			_ = bc.Cancel()
			return "", err
		}
		if !conf.expect.IsZero() && !conf.expect.Equal(d) {
			_ = bc.Cancel()
			return "", fmt.Errorf("digest mismatch, expected %s, received %s", conf.expect.String(), d.String())
		}
		err = bc.Save(d)
		if err != nil {
			return "", err
		}
		r.mod = time.Now()
		return "", nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// create a new session
	sessionID, err := genSessionID()
	if err != nil {
		return "", fmt.Errorf("failed generating sessionID: %w", err)
	}
	_, err = r.uploads.Get(sessionID)
	if err == nil {
		return "", fmt.Errorf("session ID collision")
	}
	bc, err := r.store.BlobCreate()
	if err != nil {
		return "", err
	}
	w := digest.NewWriter(bc, conf.algo)
	session := &uploadSession{
		bc:        bc,
		w:         w,
		alg:       conf.algo,
		sessionID: sessionID,
	}
	r.uploads.Set(sessionID, session)
	return sessionID, nil
}

// UploadReadFrom adds data from the reader to a given offset.
// Set offset to -1 to append to the end of the current upload.
// If the offset is specified, this verifies it is the end of the upload and otherwise fails.
func (r *Repo) UploadReadFrom(sessionID string, rdr io.Reader, offset int64) error {
	us, err := r.uploads.Get(sessionID)
	if err != nil {
		return err
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	n, err := io.Copy(us.w, rdr)
	if n > 0 {
		us.size += n
	}
	return err
}

// UploadSave verifies an upload and make the blob available.
// The value of the digest is verified before saving.
func (r *Repo) UploadSave(sessionID string, d digest.Digest) error {
	us, err := r.uploads.Get(sessionID)
	if err != nil {
		return err
	}
	if d.IsZero() {
		return fmt.Errorf("digest must be defined to save the blob")
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	// remove the upload session from the cache after saving
	defer r.uploads.Delete(sessionID)
	// save only after verifying the digest
	if us.w.Verify(d) {
		return us.bc.Save(d)
	}
	dComp, err := us.w.Digest()
	if err != nil {
		return fmt.Errorf("failed computing digest of blob: %w", err)
	}
	// handle a change in the digest algorithm
	if !dComp.Algorithm().Equal(d.Algorithm()) {
		rdr := us.bc.Reader()
		if rdr == nil {
			return fmt.Errorf("failed to get reader to recompute blob digest")
		}
		dComp, err = d.Algorithm().FromReader(rdr)
		if err != nil {
			return fmt.Errorf("failed recomputing digest of blob: %w", err)
		}
		if d.Equal(dComp) {
			r.mod = time.Now()
			return us.bc.Save(d)
		}
	}
	// failed to verify, cancel and return the error
	_ = us.bc.Cancel()
	return fmt.Errorf("digest mismatch, expected %s, received %s%.0w", d.String(), dComp.String(), types.ErrDigestInvalid)
}

// UploadStatus returns the current progress of an upload.
func (r *Repo) UploadStatus(sessionID string) (UploadStatus, error) {
	us, err := r.uploads.Get(sessionID)
	if err != nil {
		return UploadStatus{}, fmt.Errorf("session %s: %w", sessionID, err)
	}
	us.mu.Lock()
	defer us.mu.Unlock()
	return UploadStatus{Size: us.size}, nil
}

// cacheNew returns an empty cache entry ready to add entries to the index manifest list.
func cacheNew() indexCache {
	return indexCache{
		index: types.Index{
			SchemaVersion: 2,
			MediaType:     types.MediaTypeOCI1ManifestList,
			Manifests:     []types.Descriptor{},
		},
	}
}

// cacheGet loads the cache from the underlying blob store if it is not already loaded.
func (r *Repo) cacheGet(ic *indexCache) error {
	if ic == nil {
		return fmt.Errorf("nil index cache encountered")
	}
	if len(ic.raw) > 0 {
		return nil // already loaded, noop
	}
	if ic.descriptor.Digest.IsZero() {
		return fmt.Errorf("index cache cannot get a zero digest")
	}
	// load content from the cache
	rdr, err := r.store.BlobGet(ic.descriptor.Digest)
	if err != nil {
		return err
	}
	ic.raw, err = io.ReadAll(rdr)
	_ = rdr.Close()
	if err != nil {
		return err
	}
	return json.Unmarshal(ic.raw, &ic.index)
}

// cachePut rebuilds the cache from the index, and conditionally pushes it to the blob store
func (r *Repo) cachePut(ic *indexCache) error {
	if ic == nil {
		return fmt.Errorf("nil index cache encountered")
	}
	var err error
	ic.raw, err = json.Marshal(ic.index)
	if err != nil {
		return err
	}
	algo := digest.Canonical
	if !ic.descriptor.Digest.IsZero() {
		algo = ic.descriptor.Digest.Algorithm()
	}
	dig, err := algo.FromBytes(ic.raw)
	if err != nil {
		return err
	}
	desc := types.Descriptor{
		MediaType: types.MediaTypeOCI1ManifestList,
		Digest:    dig,
		Size:      int64(len(ic.raw)),
	}
	r.manifests[dig] = desc.Copy()
	ic.descriptor = desc
	if !*r.conf.Storage.ReadOnly {
		bc, err := r.store.BlobCreate()
		if err != nil {
			return fmt.Errorf("failed to create cache entry: %w", err)
		}
		_, err = bc.Write(ic.raw)
		if err != nil {
			_ = bc.Cancel()
			return fmt.Errorf("failed to write cache entry: %w", err)
		}
		err = bc.Save(dig)
		if err != nil {
			return fmt.Errorf("failed to save cache entry: %w", err)
		}
		// trim in memory cache
		ic.index = types.Index{}
		ic.raw = nil
	}
	return nil
}

// gc runs a garbage collection against the repo.
func (r *Repo) gc(cutoff time.Time) {
	if r.conf.Storage.GC.GracePeriod >= 0 {
		cutoff = time.Now().Add(r.conf.Storage.GC.GracePeriod * -1)
	}
	keep := map[digest.Digest]bool{}
	// keep all tagged manifests
	for _, dig := range r.tags {
		keep[dig] = true
	}
	// conditionally keep all untagged manifests too
	if *r.conf.Storage.GC.Untagged {
		for dig := range r.manifests {
			keep[dig] = true
		}
	}
	// retain the tag history entries
	for tag, ic := range r.history {
		if _, ok := r.tags[tag]; !ok {
			continue
		}
		keep[ic.descriptor.Digest] = true
	}
	// walk to get all digests being directly preserved
	walkList := []types.Descriptor{}
	for dig := range keep {
		if d, ok := r.manifests[dig]; ok {
			walkList = append(walkList, d)
		}
	}
	for step := range r.store.Walk(types.ParseBlobs, false, walkList...) {
		keep[step.Desc.Digest] = true
	}
	// retain referrers responses
	walkList = []types.Descriptor{}
	for subj, ic := range r.referrers {
		if keep[ic.descriptor.Digest] {
			continue // already preserved
		}
		if *r.conf.Storage.GC.ReferrersDangling || keep[subj] {
			walkList = append(walkList, ic.descriptor)
		}
	}
	for step := range r.store.Walk(types.ParseBlobs, false, walkList...) {
		keep[step.Desc.Digest] = true
	}
	_ = r.store.GC(cutoff, keep)

	// tags := map[string]bool{}
	// untagged := map[digest.Digest]types.Descriptor{}
	// referrers := map[digest.Digest]types.Descriptor{}
	// tagHistory := map[string]types.Descriptor{}
	// // walk the index for each descriptor, tracking tags, manifests to preserve, referrers, tag history, and unknown descriptors to preserve
	// for _, d := range r.index.Manifests {
	// 	if types.AnnotationsEmpty(d) {
	// 		after, err := createdAfter(d, cutoff)
	// 		if err != nil {
	// 			// save to check blob mod time
	// 			untagged[d.Digest] = d
	// 		} else if after {
	// 			keep[d.Digest] = d
	// 		}
	// 	} else if d.Annotations[types.AnnotRefName] != "" {
	// 		tags[d.Annotations[types.AnnotRefName]] = true
	// 		keep[d.Digest] = d
	// 	} else if d.Annotations[types.AnnotReferrerSubject] != "" {
	// 		if dig, err := digest.Parse(d.Annotations[types.AnnotReferrerSubject]); err == nil {
	// 			referrers[dig] = d
	// 		}
	// 	} else if d.Annotations[types.AnnotTagHistory] != "" {
	// 		tagHistory[d.Annotations[types.AnnotTagHistory]] = d
	// 	} else {
	// 		// retain manifests with any unknown annotations
	// 		keep[d.Digest] = d
	// 	}
	// }
	// // if untagged manifests are not being GCed, move them over to the keep list
	// if !*conf.Storage.GC.Untagged {
	// 	for d, desc := range untagged {
	// 		keep[d] = desc
	// 	}
	// 	untagged = nil
	// }
	// // shallow walk all untagged manifests to check their mod time
	// if len(untagged) > 0 {
	// 	for step := range backend.Walk(repo, types.ParseNone, false) {
	// 		if step.mod.IsZero() || cutoff.Before(step.mod) {
	// 			keep[step.d.Digest] = step.d
	// 		}
	// 	}
	// }
	// // deep walk all manifests that are being preserved
	// walkList := make([]types.Descriptor, 0, len(keep))
	// for _, d := range keep {
	// 	walkList = append(walkList, d)
	// }
	// for step := range backend.Walk(repo, types.ParseBlobs, false, walkList...) {
	// 	keep[step.d.Digest] = step.d
	// }
	// // deep walk all referrers pointing to preserved digests, or if GC preserves dangling referrers
	// walkList = make([]types.Descriptor, 0, len(referrers))
	// dangling := make([]types.Descriptor, 0, len(referrers))
	// for dig, d := range referrers {
	// 	if *conf.Storage.GC.ReferrersDangling {
	// 		walkList = append(walkList, d)
	// 	} else if _, ok := keep[dig]; ok {
	// 		walkList = append(walkList, d)
	// 	} else {
	// 		dangling = append(dangling, d)
	// 	}
	// }
	// for step := range backend.Walk(repo, types.ParseNone, false, dangling...) {
	// 	if step.mod.IsZero() || cutoff.Before(step.mod) {
	// 		walkList = append(walkList, step.d) // also save newly pushed referrer responses
	// 	}
	// }
	// for step := range backend.Walk(repo, types.ParseBlobs, false, walkList...) {
	// 	keep[step.d.Digest] = step.d
	// }
	// // TODO: delete tag history when repo is otherwise being deleted
	// // preserve tag history manifests, but not any child content
	// walkList = make([]types.Descriptor, 0, len(tagHistory))
	// for _, d := range tagHistory {
	// 	walkList = append(walkList, d)
	// }
	// for step := range backend.Walk(repo, types.ParseNone, false, walkList...) {
	// 	keep[step.d.Digest] = step.d
	// }
	// // return list to preserve
	// result := make([]types.Descriptor, 0, len(keep))
	// for _, d := range keep {
	// 	result = append(result, d)
	// }
	// return result, nil

	// if r.conf.Storage.GC.GracePeriod >= 0 {
	// 	cutoff = time.Now().Add(r.conf.Storage.GC.GracePeriod * -1)
	// }
	// tags := map[string]bool{}
	// manifests := map[digest.Digest]types.Descriptor{}
	// subjects := map[digest.Digest]types.Descriptor{}
	// tagHistory := map[string]types.Descriptor{}
	// keepDigests := map[digest.Digest]bool{}
	// // build a list of manifests and subjects to scan
	// for _, d := range index.Manifests {
	// 	inIndex[d.Digest] = true
	// 	keep := false
	// 	// keep tagged entries or every entry if untagged entries are not GCed
	// 	if !*conf.Storage.GC.Untagged || (d.Annotations != nil && d.Annotations[types.AnnotRefName] != "") {
	// 		keep = true
	// 	}
	// 	// keep new blobs
	// 	if !keep && conf.Storage.GC.GracePeriod >= 0 {
	// 		if meta, err := repo.blobMeta(d.Digest, locked); err == nil && meta.mod.After(cutoff) {
	// 			keep = true
	// 		}
	// 	}
	// 	// referrers responses
	// 	if d.Annotations != nil && d.Annotations[types.AnnotReferrerSubject] != "" {
	// 		dig, _ := digest.Parse(d.Annotations[types.AnnotReferrerSubject])
	// 		subjExists := (!dig.IsZero())
	// 		if _, err := repo.blobMeta(dig, locked); subjExists && err != nil {
	// 			subjExists = false
	// 		}
	// 		if *conf.Storage.GC.ReferrersWithSubj && subjExists {
	// 			// track a map of responses only preserved when their subject remains
	// 			subjects[dig] = d.Copy()
	// 			keep = false
	// 		} else if !*conf.Storage.GC.ReferrersDangling {
	// 			// keep if dangling aren't GCed
	// 			keep = true
	// 		} else if subjExists {
	// 			// subject exists but need to delete dangling
	// 			if meta, err := repo.blobMeta(d.Digest, locked); err == nil && conf.Storage.GC.GracePeriod >= 0 && meta.mod.After(cutoff) {
	// 				// always keep new entries
	// 				keep = true
	// 			} else {
	// 				// else preserve only if subject remains
	// 				subjects[dig] = d.Copy()
	// 				keep = false
	// 			}
	// 		}
	// 	}
	// 	if keep {
	// 		manifests = append(manifests, d.Copy())
	// 	}
	// }
	// seen := map[digest.Digest]bool{}
	// // walk all manifests to note seen digests
	// for len(manifests) > 0 {
	// 	// work from tail to make deletes easier
	// 	d := manifests[len(manifests)-1]
	// 	manifests = manifests[:len(manifests)-1]
	// 	inIndex[d.Digest] = true
	// 	if seen[d.Digest] {
	// 		continue
	// 	}
	// 	br, err := repo.blobGet(d.Digest, locked)
	// 	if err != nil {
	// 		continue
	// 	}
	// 	seen[d.Digest] = true
	// 	// parse manifests for descriptors (manifests, config, layers)
	// 	if types.MediaTypeIndex(d.MediaType) {
	// 		man := types.Index{}
	// 		err = json.NewDecoder(br).Decode(&man)
	// 		errClose := br.Close()
	// 		if err != nil || errClose != nil {
	// 			continue
	// 		}
	// 		for _, child := range man.Manifests {
	// 			manifests = append(manifests, child.Copy())
	// 		}
	// 	} else if types.MediaTypeImage(d.MediaType) {
	// 		man := types.Manifest{}
	// 		err = json.NewDecoder(br).Decode(&man)
	// 		errClose := br.Close()
	// 		if err != nil || errClose != nil {
	// 			continue
	// 		}
	// 		seen[man.Config.Digest] = true
	// 		for _, layer := range man.Layers {
	// 			seen[layer.Digest] = true
	// 		}
	// 	} else {
	// 		// unknown media type listed in an index, treat it as a blob
	// 		errClose := br.Close()
	// 		if errClose != nil {
	// 			continue
	// 		}
	// 	}
	// 	// if there are referrers to this manifest
	// 	if referrer, ok := subjects[d.Digest]; ok {
	// 		manifests = append(manifests, referrer)
	// 	}
	// }
	// // clean old blobs that were not seen
	// mod := false
	// blobExists := map[digest.Digest]bool{}
	// dl, err := repo.blobList(locked)
	// if err != nil {
	// 	return index, false, fmt.Errorf("failed to list blobs to GC: %w", err)
	// }
	// for _, d := range dl {
	// 	blobExists[d] = true
	// 	if seen[d] {
	// 		continue
	// 	}
	// 	bInfo, errMeta := repo.blobMeta(d, locked)
	// 	if errMeta == nil && conf.Storage.GC.GracePeriod >= 0 && bInfo.mod.After(cutoff) && !inIndex[d] {
	// 		// keep recently uploaded blobs (manifests handled above)
	// 		continue
	// 	}
	// 	// prune from index, check existence directly since some index entries may not be accessible
	// 	if _, err := index.GetDesc(d.String()); err == nil {
	// 		mod = true
	// 		index.RmDesc(types.Descriptor{Digest: d})
	// 	}
	// 	// attempt to prune from blob store, ignoring errors
	// 	_ = repo.blobDelete(d, locked)
	// }
	// // cleanup index entries without a backing blob
	// for d := range inIndex {
	// 	if !blobExists[d] {
	// 		mod = true
	// 		index.RmDesc(types.Descriptor{Digest: d})
	// 	}
	// }
	// return index, mod, nil
}

// historyAdd appends an event to the tag history.
func (r *Repo) historyAdd(tag string, event string, desc types.Descriptor) error {
	ic, ok := r.history[tag]
	if !ok {
		ic = cacheNew()
	}
	newDesc := desc.Copy()
	if newDesc.Annotations == nil {
		newDesc.Annotations = map[string]string{}
	}
	newDesc.Annotations[types.AnnotTagEvent] = event
	newDesc.Annotations[types.AnnotTagTimestamp] = reproducible.TimeNow().Format(time.RFC3339)
	ic.index.Manifests = append(ic.index.Manifests, newDesc)
	err := r.cachePut(&ic)
	if err != nil {
		return err
	}
	_, err = r.indexAddByAnnotation(ic.descriptor, types.AnnotTagHistory, tag)
	if err != nil {
		return err
	}
	return nil
}

// indexAddByDigest inserts a descriptor into the index by digest.
// The bool is true when the index is modified.
func (r *Repo) indexAddByDigest(desc types.Descriptor) (bool, error) {
	if !types.AnnotationsEmpty(desc) {
		return false, fmt.Errorf("cannot insert by digest with annotations set")
	}
	if slices.ContainsFunc(r.index.Manifests, func(cur types.Descriptor) bool { return cur.Digest.Equal(desc.Digest) && types.AnnotationsEmpty(cur) }) {
		// entry already exists, noop
		return false, nil
	}
	newDesc := desc.Copy()
	if newDesc.Annotations == nil {
		newDesc.Annotations = map[string]string{}
	}
	newDesc.Annotations[types.AnnotCreated] = reproducible.TimeNow().Format(time.RFC3339)
	r.index.Manifests = append(r.index.Manifests, newDesc)
	return true, nil
}

// indexAddByAnnotation inserts a descriptor into the index by annotation.
// Other entries with the same annotation are removed/replaced.
// When a "tag" annotation is added, the history and tag pointers are also updated.
func (r *Repo) indexAddByAnnotation(desc types.Descriptor, key, val string) (bool, error) {
	newDesc := desc.Copy()
	if newDesc.Annotations == nil {
		newDesc.Annotations = map[string]string{}
	}
	newDesc.Annotations[types.AnnotCreated] = reproducible.TimeNow().Format(time.RFC3339)
	newDesc.Annotations[key] = val
	replaced := false
	for i, cur := range r.index.Manifests {
		if cur.Same(newDesc) {
			// entry already exists, noop
			return false, nil
		}
		if annotationEq(cur, key, val) {
			// replace existing entry
			r.index.Manifests[i] = newDesc
			replaced = true
			break
		}
	}
	if !replaced {
		// add new entry
		r.index.Manifests = append(r.index.Manifests, newDesc)
	}
	if key == types.AnnotRefName {
		r.tags[val] = desc.Digest
		err := r.historyAdd(val, "created", desc)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

// indexDelByDigest removes all descriptors from the index with a given digest.
func (r *Repo) indexDelByDigest(desc types.Descriptor) (bool, error) {
	start := len(r.index.Manifests)
	// handle deleting tags
	for _, cur := range r.index.Manifests {
		if cur.Digest.Equal(desc.Digest) {
			if curTag, ok := cur.AnnotationVal(types.AnnotRefName); ok && curTag != "" {
				err := r.historyAdd(curTag, "deleted", cur)
				if err != nil {
					return true, err
				}
				delete(r.tags, curTag)
			}
		}
	}
	r.index.Manifests = slices.DeleteFunc(r.index.Manifests, func(cur types.Descriptor) bool { return cur.Digest.Equal(desc.Digest) })
	return len(r.index.Manifests) != start, nil
}

// indexDelByAnnotation removes add descriptors from the index with a matching annotation.
func (r *Repo) indexDelByAnnotation(key, val string) (bool, error) {
	histMod := false
	if key == types.AnnotRefName {
		dig, okTag := r.tags[val]
		desc, okMan := r.manifests[dig]
		if okTag && okMan {
			err := r.historyAdd(val, "deleted", desc)
			if err != nil {
				return true, err
			}
			histMod = true
		}
		delete(r.tags, val)
	}
	start := len(r.index.Manifests)
	r.index.Manifests = slices.DeleteFunc(r.index.Manifests, func(cur types.Descriptor) bool { return annotationEq(cur, key, val) })
	return histMod || len(r.index.Manifests) != start, nil
}

// referrersAdd parses a manifest for the subject and when found, adds it to the referrers response.
func (r *Repo) referrersAdd(b []byte, mt string, dig digest.Digest) (digest.Digest, bool, error) {
	subj := manifestSubject{}
	err := json.Unmarshal(b, &subj)
	if err != nil || subj.Subject == nil || subj.Subject.Digest.IsZero() {
		return digest.Digest{}, false, nil
	}
	// build the descriptor that goes in the referrers response
	at := subj.ArtifactType
	if at == "" {
		at = subj.Config.MediaType
	}
	respEntry := types.Descriptor{
		MediaType:    mt,
		ArtifactType: at,
		Digest:       dig,
		Size:         int64(len(b)),
		Annotations:  subj.Annotations,
	}
	respCache, ok := r.referrers[subj.Subject.Digest]
	if ok {
		err := r.cacheGet(&respCache)
		if err != nil && respCache.index.MediaType == "" {
			respCache = cacheNew()
		}
	} else {
		respCache = cacheNew()
	}
	if slices.ContainsFunc(respCache.index.Manifests, func(cur types.Descriptor) bool { return cur.Digest.Equal(respEntry.Digest) }) {
		// response already exists, noop
		return subj.Subject.Digest, false, nil
	}
	// update the response with this entry
	respCache.index.Manifests = append(respCache.index.Manifests, respEntry)
	err = r.cachePut(&respCache)
	if err != nil {
		return digest.Digest{}, false, fmt.Errorf("failed to update referrers response: %w", err)
	}
	r.referrers[subj.Subject.Digest] = respCache
	// update the index
	mod, err := r.indexAddByAnnotation(respCache.descriptor, types.AnnotReferrerSubject, subj.Subject.Digest.String())
	if err != nil {
		return subj.Subject.Digest, mod, err
	}
	return subj.Subject.Digest, mod, nil
}

// referrersDel pulls the manifest to parse for a subject, and when found removes it from the appropriate referrers response.
func (r *Repo) referrersDel(dig digest.Digest) (bool, error) {
	rdr, err := r.store.BlobGet(dig)
	if err != nil {
		return false, err
	}
	b, err := io.ReadAll(rdr)
	_ = rdr.Close()
	if err != nil {
		return false, err
	}
	subj := manifestSubject{}
	err = json.Unmarshal(b, &subj)
	if err != nil || subj.Subject == nil || subj.Subject.Digest.IsZero() {
		return false, nil
	}
	respCache, ok := r.referrers[subj.Subject.Digest]
	if ok {
		err := r.cacheGet(&respCache)
		if err != nil && respCache.index.MediaType == "" {
			respCache = cacheNew()
		}
	} else {
		respCache = cacheNew()
	}
	lenStart := len(respCache.index.Manifests)
	respCache.index.Manifests = slices.DeleteFunc(respCache.index.Manifests, func(cur types.Descriptor) bool { return cur.Digest.Equal(dig) })
	if len(respCache.index.Manifests) == lenStart {
		// no entries deleted, noop
		return false, nil
	}
	err = r.cachePut(&respCache)
	if err != nil {
		return false, err
	}
	r.referrers[subj.Subject.Digest] = respCache
	mod, err := r.indexAddByAnnotation(respCache.descriptor, types.AnnotReferrerSubject, subj.Subject.Digest.String())
	if err != nil {
		return mod, err
	}
	return mod, nil
}

// annotationEq returns true if the annotation is set to a given value.
func annotationEq(desc types.Descriptor, key, val string) bool {
	if desc.Annotations == nil {
		return false
	}
	curVal, ok := desc.Annotations[key]
	return ok && val == curVal
}

// createdAfter returns true when a descriptor has a created annotation after the cutoff.
func createdAfter(d types.Descriptor, cutoff time.Time) (bool, error) {
	if cutoff.IsZero() {
		return true, nil
	}
	if d.Annotations == nil || d.Annotations[types.AnnotCreated] == "" {
		return false, fmt.Errorf("no created annotation")
	}
	created, err := time.Parse(time.RFC3339, d.Annotations[types.AnnotCreated])
	if err != nil {
		return false, err
	}
	if cutoff.Before(created) {
		return true, nil
	} else {
		return false, nil
	}
}

// genSessionID returns a random ID safe for use in a URL.
func genSessionID() (string, error) {
	sb := make([]byte, 16)
	_, err := rand.Read(sb)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(sb), nil
}

// const (
// 	freqCheck  = time.Second
// 	indexFile  = "index.json"
// 	layoutFile = "oci-layout"
// 	blobsDir   = "blobs"
// 	uploadDir  = "_uploads"
// )

// var referrerTagRe = regexp.MustCompile(`^(sha256|sha512)-([0-9a-f]{64})$`)

// // Store interface is used to abstract access to a backend storage system for repositories.
// type Store interface {
// 	// RepoGet returns a repo from the store.
// 	// When finished, the method [Repo.Done] must be called.
// 	RepoGet(ctx context.Context, repoStr string) (Repo, error)

// 	// Close releases resources used by the store.
// 	// The store should not be used after being closed.
// 	Close() error
// }

// // Repo interface is used to access a CAS and the index managing known manifests.
// type Repo interface {
// 	// IndexGet returns the current top level index for a repo.
// 	IndexGet() (types.LayoutIndex, error)
// 	// IndexInsert adds a new entry to the index and writes the change to index.json.
// 	IndexInsert(desc types.Descriptor, opts ...types.LayoutIndexOpt) error
// 	// IndexRemove deletes an entry from the index and writes the change to index.json.
// 	IndexRemove(desc types.Descriptor) error

// 	// BlobGet returns a reader to an entry from the CAS.
// 	BlobGet(d digest.Digest) (io.ReadSeekCloser, error)
// 	// BlobCreate is used to create a new blob.
// 	BlobCreate(opts ...BlobOpt) (BlobCreator, string, error)
// 	// BlobDelete removes an entry from the CAS.
// 	BlobDelete(d digest.Digest) error
// 	// BlobSession is used to retrieve an upload session
// 	BlobSession(sessionID string) (BlobCreator, error)

// 	// Done indicates the routine using this repo is finished.
// 	// This must be called exactly once for every instance of [Store.RepoGet].
// 	Done()

// 	// blobDelete is the internal method for deleting a blob.
// 	blobDelete(d digest.Digest, locked bool) error
// 	// blobGet is an internal method for accessing blobs from other store methods.
// 	blobGet(d digest.Digest, locked bool) (io.ReadSeekCloser, error)
// 	// blobList returns a list of all known blobs in the repo
// 	blobList(locked bool) ([]digest.Digest, error)
// 	// blobMeta returns metadata on a blob.
// 	blobMeta(d digest.Digest, locked bool) (blobMeta, error)
// 	// gc runs the garbage collect
// 	gc() error
// }

// type BlobOpt func(*blobConfig) error

// type blobConfig struct {
// 	algo   digest.Algorithm
// 	expect digest.Digest
// }

// func BlobWithAlgorithm(a digest.Algorithm) BlobOpt {
// 	return func(bc *blobConfig) error {
// 		bc.algo = a
// 		return nil
// 	}
// }

// func BlobWithDigest(d digest.Digest) BlobOpt {
// 	return func(bc *blobConfig) error {
// 		if d.IsZero() {
// 			return fmt.Errorf("invalid digest: %s", d.String())
// 		}
// 		bc.expect = d
// 		bc.algo = d.Algorithm()
// 		return nil
// 	}
// }

// // BlobCreator is used to upload new blobs.
// type BlobCreator interface {
// 	// WriteCloser is used to push the blob content.
// 	io.WriteCloser
// 	// Cancel is used to stop an upload.
// 	Cancel() error
// 	// Size reports the number of bytes pushed.
// 	Size() int64
// 	// Digest is used to get the current digest of the content.
// 	Digest() digest.Digest
// 	// Verify ensures a digest matches the content.
// 	Verify(digest.Digest) error
// 	// ChangeAlgorithm modifies the digest algorithm. This may only be rejected after the first write.
// 	ChangeAlgorithm(digest.Algorithm) error
// }

// // blobMeta includes metadata available for blobs.
// type blobMeta struct {
// 	mod time.Time
// }

// // Opts includes options for the directory store.
// type Opts func(*storeConf)

// type storeConf struct {
// 	log *slog.Logger
// }

// // WithLog includes a logger on the directory store.
// func WithLog(log *slog.Logger) Opts {
// 	return func(sc *storeConf) {
// 		sc.log = log
// 	}
// }

// // indexIngest processes an index.json file, adding child descriptors, and converting referrers if appropriate.
// // return is true when index has been modified.
// func indexIngest(repo Repo, index *types.LayoutIndex, conf config.Config, locked bool) (bool, error) {
// 	mod := false
// 	// error if referrer API not enabled and annotation indicates this is already converted, this repo should not writable
// 	if !*conf.API.Referrer.Enabled && index.Annotations != nil && index.Annotations[types.AnnotReferrerConvert] == "true" {
// 		return mod, fmt.Errorf("index.json has referrers converted with the API disabled")
// 	}
// 	// ensure index has schema and media type
// 	if index.SchemaVersion != 2 {
// 		index.SchemaVersion = 2
// 		mod = true
// 	}
// 	if index.MediaType != types.MediaTypeOCI1ManifestList {
// 		index.MediaType = types.MediaTypeOCI1ManifestList
// 		mod = true
// 	}

// 	seen := map[digest.Digest]bool{}
// 	scanChildren := []types.Descriptor{}
// 	referrerResponse := map[string]types.Descriptor{}
// 	digestTags := []types.Descriptor{}
// 	// loop over manifests
// 	for _, desc := range index.Manifests {
// 		seen[desc.Digest] = true
// 		if desc.MediaType == types.MediaTypeOCI1ManifestList && desc.Annotations != nil {
// 			if referrerTagRe.MatchString(desc.Annotations[types.AnnotRefName]) {
// 				digestTags = append(digestTags, desc)
// 			}
// 			if desc.Annotations[types.AnnotReferrerSubject] != "" {
// 				referrerResponse[desc.Annotations[types.AnnotReferrerSubject]] = desc
// 			}
// 		}
// 		if types.MediaTypeIndex(desc.MediaType) {
// 			scanChildren = append(scanChildren, desc)
// 		}
// 	}

// 	// convert referrers
// 	if *conf.API.Referrer.Enabled && (index.Annotations == nil || index.Annotations[types.AnnotReferrerConvert] != "true") {
// 		// for each fallback tag, validate it
// 		addResp := map[string][]types.Descriptor{}
// 		rmDesc := []types.Descriptor{}
// 		for _, desc := range digestTags {
// 			curResp, err := repoGetIndex(repo, desc, locked)
// 			if err != nil || curResp.Manifests == nil {
// 				continue
// 			}
// 			valid, refSubj, refResp := indexValidReferrer(repo, curResp, locked)
// 			// check for a different response already in the index
// 			if valid {
// 				if resp, ok := referrerResponse[refSubj.String()]; ok && !resp.Digest.Equal(desc.Digest) {
// 					valid = false
// 				}
// 			}
// 			// if the response is good, convert to a referrer
// 			if valid {
// 				newDesc := desc
// 				newDesc.Annotations = map[string]string{
// 					types.AnnotReferrerSubject: refSubj.String(),
// 				}
// 				index.AddDesc(newDesc)
// 				mod = true
// 			}
// 			// if the response cannot be quickly converted, save for later
// 			if !valid {
// 				for refSubj := range refResp {
// 					addResp[refSubj.String()] = append(addResp[refSubj.String()], refResp[refSubj]...)
// 				}
// 				rmDesc = append(rmDesc, desc)
// 			}
// 		}

// 		// generate new responses when needed
// 		for subj, respList := range addResp {
// 			if refDesc, ok := referrerResponse[subj]; ok {
// 				resp, err := repoGetIndex(repo, refDesc, locked)
// 				if err == nil && resp.Manifests != nil {
// 					respList = append(respList, resp.Manifests...)
// 				}
// 			}
// 			resp := types.Index{
// 				SchemaVersion: 2,
// 				MediaType:     types.MediaTypeOCI1ManifestList,
// 				Manifests:     referrerListDedup(respList),
// 			}
// 			respRaw, err := json.Marshal(resp)
// 			if err != nil {
// 				return mod, fmt.Errorf("failed to marshal referrers response: %w", err)
// 			}
// 			dig, err := digest.Canonical.FromBytes(respRaw)
// 			if err != nil {
// 				return mod, fmt.Errorf("failed to compute digest for referrers response: %w", err)
// 			}
// 			bc, _, err := repo.BlobCreate(BlobWithDigest(dig))
// 			if err != nil {
// 				return mod, err
// 			}
// 			_, err = bc.Write(respRaw)
// 			if err != nil {
// 				_ = bc.Close()
// 				return mod, err
// 			}
// 			err = bc.Close()
// 			if err != nil {
// 				return mod, err
// 			}
// 			index.AddDesc(types.Descriptor{
// 				MediaType: types.MediaTypeOCI1ManifestList,
// 				Digest:    dig,
// 				Size:      int64(len(respRaw)),
// 				Annotations: map[string]string{
// 					types.AnnotReferrerSubject: subj,
// 				},
// 			})
// 			mod = true
// 		}
// 		// cleanup processed fallback tags
// 		for _, d := range rmDesc {
// 			index.RmDesc(d)
// 		}
// 		if index.Annotations == nil {
// 			index.Annotations = map[string]string{types.AnnotReferrerConvert: "true"}
// 		} else {
// 			index.Annotations[types.AnnotReferrerConvert] = "true"
// 		}
// 		mod = true
// 	}

// 	// load child descriptors
// 	for len(scanChildren) > 0 {
// 		childIndex, err := repoGetIndex(repo, scanChildren[0], locked)
// 		if err != nil {
// 			scanChildren = scanChildren[1:]
// 			continue
// 		}
// 		if childIndex.Manifests != nil {
// 			for _, desc := range childIndex.Manifests {
// 				if !seen[desc.Digest] {
// 					index.AddChildren([]types.Descriptor{desc})
// 					if types.MediaTypeIndex(desc.MediaType) {
// 						scanChildren = append(scanChildren, desc)
// 					}
// 					seen[desc.Digest] = true
// 				}
// 			}
// 		}
// 		scanChildren = scanChildren[1:]
// 	}

// 	return mod, nil
// }

// // indexValidReferrer checks all descriptors in an index to be correct for the referrer response.
// // Any entries for a different subject, or with incorrect values (pulled up artifactType and annotations) are flagged as invalid.
// // The return is true for valid responses, the digest is for the subject if valid.
// // The returned map is of subjects with a list of descriptors to include in the referrers response to that subject.
// // Errors getting manifests are ignored and those descriptors referencing those manifests are discarded.
// func indexValidReferrer(repo Repo, index types.Index, locked bool) (bool, digest.Digest, map[digest.Digest][]types.Descriptor) {
// 	var subject digest.Digest
// 	valid := true
// 	responses := map[digest.Digest][]types.Descriptor{}
// 	for _, desc := range index.Manifests {
// 		rdr, err := repo.blobGet(desc.Digest, locked)
// 		if err != nil {
// 			// errors result in entry being dropped from response list
// 			valid = false
// 			continue
// 		}
// 		raw, err := io.ReadAll(rdr)
// 		_ = rdr.Close()
// 		if err != nil {
// 			valid = false
// 			continue
// 		}
// 		refSubj, refDesc, err := types.ManifestReferrerDescriptor(raw, desc)
// 		if err != nil {
// 			valid = false
// 			continue
// 		}
// 		// ensure all referrers point to the same subject
// 		if subject.IsZero() {
// 			subject = refSubj.Digest
// 		} else if !subject.Equal(refSubj.Digest) {
// 			valid = false
// 		}
// 		// ensure all descriptors match expected contents
// 		if valid {
// 			if desc.MediaType != refDesc.MediaType || desc.Size != refDesc.Size || desc.ArtifactType != refDesc.ArtifactType || len(desc.Annotations) != len(refDesc.Annotations) {
// 				valid = false
// 			} else if refDesc.Annotations != nil {
// 				for k, v := range refDesc.Annotations {
// 					if desc.Annotations[k] != v {
// 						valid = false
// 					}
// 				}
// 			}
// 		}
// 		// add descriptor to the list of referrers for this digest
// 		responses[refSubj.Digest] = append(responses[refSubj.Digest], refDesc)
// 	}
// 	if !valid {
// 		subject = digest.Digest{}
// 	}
// 	return valid, subject, responses
// }

// func layoutVerify(b []byte) bool {
// 	l := types.Layout{}
// 	err := json.Unmarshal(b, &l)
// 	if err != nil {
// 		return false
// 	}
// 	if l.Version != types.LayoutVersion {
// 		return false
// 	}
// 	return true
// }

// func referrerListDedup(rl []types.Descriptor) []types.Descriptor {
// 	if rl == nil {
// 		return nil
// 	}
// 	seen := map[digest.Digest]bool{}
// 	i := 0
// 	for i < len(rl) {
// 		if seen[rl[i].Digest] {
// 			// delete entry from slice
// 			rl[i] = rl[len(rl)-1]
// 			rl = rl[:len(rl)-1]
// 			continue
// 		}
// 		seen[rl[i].Digest] = true
// 		i++
// 	}
// 	return rl
// }

// func repoGetIndex(repo Repo, d types.Descriptor, locked bool) (types.Index, error) {
// 	i := types.Index{}
// 	rdr, err := repo.blobGet(d.Digest, locked)
// 	if err != nil {
// 		return i, err
// 	}
// 	err = json.NewDecoder(rdr).Decode(&i)
// 	_ = rdr.Close()
// 	if err != nil {
// 		return i, err
// 	}
// 	return i, nil
// }
