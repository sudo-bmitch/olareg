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

// Package store is a minimal interface on top of an OCI Layout.
package store

import (
	"encoding/json"
	"io"
	"iter"
	"log/slog"
	"slices"
	"time"

	"github.com/olareg/olareg/config"
	"github.com/olareg/olareg/types"
	digest "github.com/sudo-bmitch/oci-digest"
)

const (
	freqCheck  = time.Second
	indexFile  = "index.json"
	layoutFile = "oci-layout"
	blobsDir   = "blobs"
	uploadDir  = "_uploads"
)

var backends = map[string]func(conf config.ConfigStorage, opts ...Opts) (Store, error){}

func New(conf config.ConfigStorage, opts ...Opts) (Store, error) {
	name := conf.StoreType
	fn, ok := backends[name]
	if !ok {
		return nil, types.ErrNotFound
	}
	return fn(conf, opts...)
}

func Register(name string, newFn func(conf config.ConfigStorage, opts ...Opts) (Store, error)) {
	backends[name] = newFn
}

func RegisterDefaults() {
	backends[config.StoreMem] = newMem
	backends[config.StoreDir] = newDir
}

// Opts includes options for initializing the included stores.
type Opts func(*OptParams)

type OptParams struct {
	log *slog.Logger
}

// WithLog sets the logger for the included stores.
func WithLog(log *slog.Logger) Opts {
	return func(sc *OptParams) {
		sc.log = log
	}
}

// Store interface is used to abstract access to a backend storage system for repositories.
type Store interface {
	// RepoGet returns a repo.
	// The returned interface should remain valid until [Repo.Close] is called.
	RepoGet(repo string) (Repo, error)

	// Close indicates the store is no longer needed and may free up any resources.
	// Future calls to the store or any contained repos may fail.
	Close() error
}

// Repo interface is used to abstract access to each OCI Layout directory.
type Repo interface {
	// IndexGet returns the top level index.json file contents.
	IndexGet() (types.LayoutIndex, error)

	// IndexSet returns the top level index.json file contents.
	IndexSet(types.LayoutIndex) error

	// BlobCreate is used to create a new blob.
	BlobCreate() (BlobCreator, error)

	// BlobDelete removes an entry from the CAS.
	BlobDelete(d digest.Digest) error

	// BlobGet returns a reader to an entry from the CAS.
	BlobGet(d digest.Digest) (io.ReadSeekCloser, error)

	// BlobMeta returns metadata on a blob.
	BlobMeta(d digest.Digest) (BlobMeta, error)

	// TODO: Add a `BlobPut(d digest.Digest, b []byte) error` that writes the blob directly without the BlobCreator

	// Walk is used to traverse the contents of a repo.
	// If a list of descriptors is not provided, the index.json contents will be traversed.
	// Descriptors that are not found will be silently skipped.
	Walk(depth types.ManifestParseDepth, retReader bool, descList ...types.Descriptor) iter.Seq[WalkStep]

	// GC (garbage collect) cleans unmarked blobs that have been created before the cutoff time.
	GC(cutoff time.Time, keepDig map[digest.Digest]bool) error

	// Close indicates the repo is no longer being accessed and resources may be freed.
	Close() error
}

// BlobMeta contains metadata on a blob.
type BlobMeta struct {
	Mod  time.Time
	Size int64
}

// BlobCreator is used to upload new blobs.
type BlobCreator interface {
	// Writer is used to push the blob content.
	io.Writer
	// Reader returns a reader from the start of the blob.
	// The reader should always be fully consumed before calling Write again.
	Reader() io.Reader
	// Save is used to store the blob to a given digest value.
	// The store is not required to verify this value.
	Save(digest.Digest) error
	// Cancel is used to stop an upload.
	Cancel() error
}

// WalkStep is returned from each iteration of a [Backend.Walk]
type WalkStep struct {
	Desc types.Descriptor
	Rdr  io.ReadSeekCloser
	Meta BlobMeta
}

// // GCMark determines all descriptors that are reachable in a specified repo.
// // These should be excluded from any garbage collection "sweep" steps.
// // Content being created during this mark may not be returned and should not be pruned by a sweep.
// // Pruning functions calling this should sweep both their blob store and index.json.
// func GCMark(backend Backend, repo string, conf config.Config) ([]types.Descriptor, error) {
// 	var cutoff time.Time
// 	if conf.Storage.GC.GracePeriod >= 0 {
// 		cutoff = time.Now().Add(conf.Storage.GC.GracePeriod * -1)
// 	}
// 	tags := map[string]bool{}
// 	untagged := map[digest.Digest]types.Descriptor{}
// 	referrers := map[digest.Digest]types.Descriptor{}
// 	tagHistory := map[string]types.Descriptor{}
// 	keep := map[digest.Digest]types.Descriptor{}
// 	// walk the index for each descriptor, tracking tags, manifests to preserve, referrers, tag history, and unknown descriptors to preserve
// 	ind, err := backend.IndexGet(repo)
// 	if err != nil {
// 		return nil, err
// 	}
// 	for _, d := range ind.Manifests {
// 		if types.AnnotationsEmpty(d) {
// 			after, err := createdAfter(d, cutoff)
// 			if err != nil {
// 				// save to check blob mod time
// 				untagged[d.Digest] = d
// 			} else if after {
// 				keep[d.Digest] = d
// 			}
// 		} else if d.Annotations[types.AnnotRefName] != "" {
// 			tags[d.Annotations[types.AnnotRefName]] = true
// 			keep[d.Digest] = d
// 		} else if d.Annotations[types.AnnotReferrerSubject] != "" {
// 			if dig, err := digest.Parse(d.Annotations[types.AnnotReferrerSubject]); err == nil {
// 				referrers[dig] = d
// 			}
// 		} else if d.Annotations[types.AnnotTagHistory] != "" {
// 			tagHistory[d.Annotations[types.AnnotTagHistory]] = d
// 		} else {
// 			// retain manifests with any unknown annotations
// 			keep[d.Digest] = d
// 		}
// 	}
// 	// if untagged manifests are not being GCed, move them over to the keep list
// 	if !*conf.Storage.GC.Untagged {
// 		for d, desc := range untagged {
// 			keep[d] = desc
// 		}
// 		untagged = nil
// 	}
// 	// shallow walk all untagged manifests to check their mod time
// 	if len(untagged) > 0 {
// 		for step := range backend.Walk(repo, types.ParseNone, false) {
// 			if step.mod.IsZero() || cutoff.Before(step.mod) {
// 				keep[step.d.Digest] = step.d
// 			}
// 		}
// 	}
// 	// deep walk all manifests that are being preserved
// 	walkList := make([]types.Descriptor, 0, len(keep))
// 	for _, d := range keep {
// 		walkList = append(walkList, d)
// 	}
// 	for step := range backend.Walk(repo, types.ParseBlobs, false, walkList...) {
// 		keep[step.d.Digest] = step.d
// 	}
// 	// deep walk all referrers pointing to preserved digests, or if GC preserves dangling referrers
// 	walkList = make([]types.Descriptor, 0, len(referrers))
// 	dangling := make([]types.Descriptor, 0, len(referrers))
// 	for dig, d := range referrers {
// 		if *conf.Storage.GC.ReferrersDangling {
// 			walkList = append(walkList, d)
// 		} else if _, ok := keep[dig]; ok {
// 			walkList = append(walkList, d)
// 		} else {
// 			dangling = append(dangling, d)
// 		}
// 	}
// 	for step := range backend.Walk(repo, types.ParseNone, false, dangling...) {
// 		if step.mod.IsZero() || cutoff.Before(step.mod) {
// 			walkList = append(walkList, step.d) // also save newly pushed referrer responses
// 		}
// 	}
// 	for step := range backend.Walk(repo, types.ParseBlobs, false, walkList...) {
// 		keep[step.d.Digest] = step.d
// 	}
// 	// TODO: delete tag history when repo is otherwise being deleted
// 	// preserve tag history manifests, but not any child content
// 	walkList = make([]types.Descriptor, 0, len(tagHistory))
// 	for _, d := range tagHistory {
// 		walkList = append(walkList, d)
// 	}
// 	for step := range backend.Walk(repo, types.ParseNone, false, walkList...) {
// 		keep[step.d.Digest] = step.d
// 	}
// 	// return list to preserve
// 	result := make([]types.Descriptor, 0, len(keep))
// 	for _, d := range keep {
// 		result = append(result, d)
// 	}
// 	return result, nil
// }

// func createdAfter(d types.Descriptor, cutoff time.Time) (bool, error) {
// 	if cutoff.IsZero() {
// 		return true, nil
// 	}
// 	if d.Annotations == nil || d.Annotations[types.AnnotCreated] == "" {
// 		return false, fmt.Errorf("no created annotation")
// 	}
// 	created, err := time.Parse(time.RFC3339, d.Annotations[types.AnnotCreated])
// 	if err != nil {
// 		return false, err
// 	}
// 	if cutoff.Before(created) {
// 		return true, nil
// 	} else {
// 		return false, nil
// 	}
// }

// TODO: move to types/layout.go
func layoutVerify(b []byte) bool {
	l := types.Layout{}
	err := json.Unmarshal(b, &l)
	if err != nil {
		return false
	}
	if l.Version != types.LayoutVersion {
		return false
	}
	return true
}

// TODO: remove
func stringsHasAny(list []string, check ...string) bool {
	for _, c := range check {
		if slices.Contains(list, c) {
			return true
		}
	}
	return false
}
