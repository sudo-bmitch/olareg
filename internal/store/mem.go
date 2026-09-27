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

package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/olareg/olareg/config"
	"github.com/olareg/olareg/types"
	digest "github.com/sudo-bmitch/oci-digest"
)

// mem is the in memory representation of an OCI Layout.
// When holding multiple mutex locks simultaneously, always start from fine grain lock first (memRepoUpload, then memRepo, then mem) to avoid deadlocks.

type mem struct {
	mu    sync.Mutex
	repos map[string]*memRepo
	blobs map[digest.Digest]*memBlob
	log   *slog.Logger
	conf  config.ConfigStorage
	next  Store
}

type memRepo struct {
	mu      sync.Mutex
	m       *mem
	repo    string
	timeMod time.Time // TODO: can probably drop this
	index   types.LayoutIndex
	blobs   map[digest.Digest]*memRepoBlob
	uploads []*memRepoUpload // TODO: can probably drop this
	next    Repo
}

type memBlob struct {
	b        []byte
	refCount int
}

type memRepoBlob struct {
	b   []byte
	mod time.Time
}

type memRepoUpload struct {
	mu     sync.Mutex
	buffer *bytes.Buffer
	mr     *memRepo
}

// newMem is registered with RegisterDefaults to return a new
func newMem(conf config.ConfigStorage, opts ...Opts) (Store, error) {
	op := OptParams{
		log: slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(&op)
	}
	m := &mem{
		repos: map[string]*memRepo{},
		blobs: map[digest.Digest]*memBlob{},
		log:   op.log,
		conf:  conf,
	}
	if conf.RootDir != "" {
		// configure directory backend as a fallthrough storage
		dir, err := newDir(conf, opts...)
		if err != nil {
			return nil, err
		}
		m.next = dir
	}
	return m, nil
}

// repoGet returns an existing repo or initializes a new one.
func (m *mem) RepoGet(repo string) (Repo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mr, ok := m.repos[repo]; ok {
		return mr, nil
	}
	// create an empty repo
	mr := &memRepo{
		m:    m,
		repo: repo,
		index: types.LayoutIndex{
			Index: types.Index{
				SchemaVersion: 2,
				MediaType:     types.MediaTypeOCI1ManifestList,
				Manifests:     []types.Descriptor{},
				Annotations:   map[string]string{},
			},
		},
		blobs:   map[digest.Digest]*memRepoBlob{},
		uploads: []*memRepoUpload{},
	}
	if m.next != nil {
		if nextRepo, err := m.next.RepoGet(repo); err == nil {
			mr.next = nextRepo
			// initialize the index
			idx, err := nextRepo.IndexGet()
			if err != nil {
				return nil, fmt.Errorf("failed to initialize index: %v", err)
			}
			mr.index = idx
		}
	}
	m.repos[repo] = mr
	return mr, nil
}

// Close is used to free up backend resources.
// Further calls to Backend methods may fail after this is run.
func (m *mem) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs = map[digest.Digest]*memBlob{}
	m.repos = map[string]*memRepo{}
	return nil
}

// IndexGet returns the top level index.json file contents.
func (mr *memRepo) IndexGet() (types.LayoutIndex, error) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return mr.index.Copy(), nil
}

// IndexSet returns the top level index.json file contents.
func (mr *memRepo) IndexSet(i types.LayoutIndex) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.index = i.Copy()
	return nil
}

// BlobCreate is used to create a new blob.
func (mr *memRepo) BlobCreate() (BlobCreator, error) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	buffer := &bytes.Buffer{}
	bc := &memRepoUpload{
		buffer: buffer,
		mr:     mr,
	}
	mr.timeMod = time.Now()
	mr.uploads = append(mr.uploads, bc)
	return bc, nil
}

// BlobDelete removes an entry from the CAS.
func (mr *memRepo) BlobDelete(d digest.Digest) error {
	if d.IsZero() {
		return fmt.Errorf("invalid digest: %s", d.String())
	}
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if b, ok := mr.blobs[d]; ok {
		if b != nil {
			if mr.next != nil {
				mr.blobs[d] = nil
			} else {
				delete(mr.blobs, d)
			}
			mr.m.mu.Lock()
			if mr.m.blobs[d] != nil && mr.m.blobs[d].refCount > 1 {
				mr.m.blobs[d].refCount--
			} else {
				delete(mr.m.blobs, d)
			}
			mr.m.mu.Unlock()
		} else {
			return types.ErrNotFound
		}
	} else if mr.next != nil {
		b, err := mr.next.BlobGet(d)
		if errors.Is(err, types.ErrNotFound) {
			return types.ErrNotFound
		}
		b.Close()
		// blob exists in fallback, only delete it from the memory store with a flag on the repo
		mr.blobs[d] = nil
	} else {
		return types.ErrNotFound
	}
	mr.timeMod = time.Now()
	mr.m.log.Debug("blob deleted", "repo", mr.repo, "digest", d.String())
	return nil
}

// BlobGet returns a reader to an entry from the CAS.
func (mr *memRepo) BlobGet(d digest.Digest) (io.ReadSeekCloser, error) {
	if d.IsZero() {
		return nil, fmt.Errorf("invalid digest: %s", d.String())
	}
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if b, ok := mr.blobs[d]; ok {
		if b != nil && ok {
			return types.BytesReadCloser{Reader: bytes.NewReader(b.b)}, nil
		} else {
			// blob explicitly deleted, do not fallback
			return nil, types.ErrNotFound
		}
	} else if mr.next != nil {
		// fallback to underlying storage
		return mr.next.BlobGet(d)
	} else {
		return nil, types.ErrNotFound
	}
}

// BlobMeta returns metadata on a blob.
func (mr *memRepo) BlobMeta(d digest.Digest) (BlobMeta, error) {
	if d.IsZero() {
		return BlobMeta{}, fmt.Errorf("invalid digest: %s", d.String())
	}
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if b, ok := mr.blobs[d]; ok {
		if b != nil && ok {
			return BlobMeta{Mod: b.mod, Size: int64(len(b.b))}, nil
		} else {
			// blob explicitly deleted, do not fallback
			return BlobMeta{}, types.ErrNotFound
		}
	} else if mr.next != nil {
		// fallback to underlying storage
		return mr.next.BlobMeta(d)
	} else {
		return BlobMeta{}, types.ErrNotFound
	}
}

// // Prune is used to run a cleaning of the backend repos and blob stores.
// func (m *mem) Prune() error {
// 	gcDigest := map[digest.Digest]int{}
// 	// track the cutoff from the start of the prune
// 	cutoff := time.Now()
// 	if m.conf.GC.GracePeriod >= 0 {
// 		cutoff = cutoff.Add(m.conf.GC.GracePeriod * -1)
// 	}
// 	// make a list of repos so we don't need to hold the lock on mem
// 	m.mu.Lock()
// 	repoNames := make([]string, 0, len(m.repos))
// 	for r := range m.repos {
// 		repoNames = append(repoNames, r)
// 	}
// 	m.mu.Unlock()
// 	// process each repo
// 	for _, repo := range repoNames {
// 		// mark all descriptors that should be preserved and convert to a list of digest
// 		markedDesc, err := GCMark(m, repo, m.conf)
// 		if err != nil {
// 			continue
// 		}
// 		markedDig := make(map[digest.Digest]bool, len(markedDesc))
// 		for _, desc := range markedDesc {
// 			markedDig[desc.Digest] = true
// 		}
// 		mr := m.repoGet(repo)
// 		mr.mu.Lock()
// 		// clean index.json, parse in reverse so that deletes do not throw off the position in the array
// 		for i := len(mr.index.Manifests) - 1; i >= 0; i-- {
// 			desc := mr.index.Manifests[i]
// 			if markedDig[desc.Digest] {
// 				continue
// 			}
// 			if mrb, ok := mr.blobs[desc.Digest]; ok && cutoff.Before(mrb.mod) {
// 				continue
// 			}
// 			mr.index.Manifests = slices.Delete(mr.index.Manifests, i, i+1)
// 		}
// 		// clean blobs in the repo and track when they are deleted
// 		for dig, mrb := range mr.blobs {
// 			if markedDig[dig] || cutoff.Before(mrb.mod) {
// 				continue
// 			}
// 			delete(mr.blobs, dig)
// 			gcDigest[dig]++
// 		}
// 		mr.mu.Unlock()
// 	}
// 	// clean shared blob store
// 	m.mu.Lock()
// 	for dig, count := range gcDigest {
// 		if mb, ok := m.blobs[dig]; ok && mb.refCount > count {
// 			mb.refCount -= count
// 		} else {
// 			delete(m.blobs, dig)
// 		}
// 	}
// 	m.mu.Unlock()
// 	return nil
// }

// Walk is used to traverse the contents of a repo.
func (mr *memRepo) Walk(depth types.ManifestParseDepth, retReader bool, descList ...types.Descriptor) iter.Seq[WalkStep] {
	if len(descList) == 0 {
		mr.mu.Lock()
		descList = make([]types.Descriptor, len(mr.index.Manifests))
		for i, d := range mr.index.Manifests {
			descList[i] = d.Copy()
		}
		mr.mu.Unlock()
	}
	return func(yield func(WalkStep) bool) {
		for i := 0; i < len(descList); i++ { // descList may be appended in this loop
			d := descList[i]
			step := WalkStep{Desc: d}
			var raw []byte
			mr.mu.Lock()
			mrb, ok := mr.blobs[d.Digest]
			mr.mu.Unlock()
			if ok && mrb != nil {
				step.Meta = BlobMeta{Mod: mrb.mod, Size: int64(len(mrb.b))}
				if retReader {
					step.Rdr = types.BytesReadCloser{Reader: bytes.NewReader(mrb.b)}
				}
				if types.MediaTypeManifest(d.MediaType) {
					raw = mrb.b
				}
			} else if !ok && mr.next != nil {
				// fall through to next store
				meta, err := mr.next.BlobMeta(d.Digest)
				if err != nil {
					continue
				}
				step.Meta = meta
				if types.MediaTypeManifest(d.MediaType) {
					rdr, err := mr.next.BlobGet(d.Digest)
					if err != nil {
						continue
					}
					raw, err = io.ReadAll(rdr)
					_ = rdr.Close()
					if err != nil {
						continue
					}
					if retReader {
						step.Rdr = types.BytesReadCloser{Reader: bytes.NewReader(raw)}
					}
				} else if retReader {
					rdr, err := mr.next.BlobGet(d.Digest)
					if err != nil {
						continue
					}
					step.Rdr = rdr
				}
			} else {
				// skip deleted or missing entries
				continue
			}
			if !yield(step) {
				return
			}
			if raw != nil {
				addDesc, _ := types.ManifestParseDescriptors(raw, d, depth)
				if len(addDesc) > 0 {
					descList = append(descList, addDesc...)
				}
			}
		}
	}
}

// GC (garbage collect) cleans unmarked blobs that have been created before the cutoff time.
func (mr *memRepo) GC(cutoff time.Time, keepDig map[digest.Digest]bool) error {
	// clean old unmarked blobs from the repo
	rmDigList := map[digest.Digest]bool{}
	mr.mu.Lock()
	for cur, mrb := range mr.blobs {
		if !keepDig[cur] && mrb.mod.Before(cutoff) {
			rmDigList[cur] = true
			delete(mr.blobs, cur)
		}
	}
	mr.mu.Unlock()
	if len(rmDigList) == 0 {
		return nil
	}
	// clean from shared blob store
	m := mr.m
	m.mu.Lock()
	for cur := range rmDigList {
		if mb, ok := m.blobs[cur]; ok && mb.refCount > 1 {
			mb.refCount--
		} else {
			delete(m.blobs, cur)
		}
	}
	m.mu.Unlock()
	return nil
}

// Close indicates the repo is no longer being accessed and resources may be freed.
func (mr *memRepo) Close() error {
	return nil
}

// Cancel is used to stop an upload.
func (mru *memRepoUpload) Cancel() error {
	mru.mu.Lock()
	defer mru.mu.Unlock()
	mr := mru.mr
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.uploads = slices.DeleteFunc(mr.uploads, func(cur *memRepoUpload) bool { return cur == mru })
	return nil
}

// Reader returns a new reader from the uploaded buffer.
func (mru *memRepoUpload) Reader() io.Reader {
	mru.mu.Lock()
	defer mru.mu.Unlock()
	return bytes.NewBuffer(mru.buffer.Bytes())
}

// Save is used to save an upload to a CAS digest.
func (mru *memRepoUpload) Save(d digest.Digest) error {
	if d.IsZero() {
		return types.ErrDigestInvalid
	}
	mru.mu.Lock()
	defer mru.mu.Unlock()
	mrb := &memRepoBlob{
		b:   mru.buffer.Bytes(),
		mod: time.Now(),
	}
	mru.buffer = &bytes.Buffer{} // reset the buffer so stored byte slice cannot be modified
	mr := mru.mr
	mr.mu.Lock()
	defer mr.mu.Unlock()
	m := mr.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.blobs[d]; ok {
		if !bytes.Equal(existing.b, mrb.b) {
			return fmt.Errorf("digest collision encountered pushing to %s: %s", mr.repo, d.String())
		}
		mrb.b = existing.b
		m.blobs[d].refCount++
	} else {
		m.blobs[d] = &memBlob{
			b:        mrb.b,
			refCount: 1,
		}
	}
	mr.blobs[d] = mrb
	mr.timeMod = mrb.mod
	m.log.Debug("blob created", "repo", mr.repo, "digest", d.String())
	mr.uploads = slices.DeleteFunc(mr.uploads, func(cur *memRepoUpload) bool { return cur == mru })
	return nil
}

// Write sends data to the buffer.
func (mru *memRepoUpload) Write(p []byte) (int, error) {
	mru.mu.Lock()
	defer mru.mu.Unlock()
	return mru.buffer.Write(p)
}
