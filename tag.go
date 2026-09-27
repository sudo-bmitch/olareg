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
	"sort"
	"strconv"
	"strings"

	"github.com/olareg/olareg/types"
)

func (s *Server) tagList(repoStr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repo, err := s.backend.RepoGet(repoStr)
		if err != nil {
			if errors.Is(err, types.ErrRepoNotAllowed) {
				w.WriteHeader(http.StatusBadRequest)
				_ = types.ErrRespJSON(w, types.ErrInfoNameInvalid("repository name is not allowed"))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			s.log.Info("failed to get repo", "err", err, "repo", repoStr)
			return
		}
		tags, err := repo.TagList()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			s.log.Info("failed to list tags", "err", err, "repo", repoStr)
			return
		}
		last := r.URL.Query().Get("last")
		w.Header().Add("Content-Type", "application/json")
		tl := types.TagList{
			Name: repoStr,
			Tags: []string{},
		}
		for t, _ := range tags {
			if strings.Compare(last, t) < 0 {
				tl.Tags = append(tl.Tags, t)
			}
		}
		sort.Strings(tl.Tags)
		n := r.URL.Query().Get("n")
		if n != "" {
			if nInt, err := strconv.Atoi(n); err == nil && len(tl.Tags) > nInt {
				tl.Tags = tl.Tags[:nInt]
				// add next header for pagination
				next := r.URL
				q := next.Query()
				q.Set("last", tl.Tags[len(tl.Tags)-1])
				next.RawQuery = q.Encode()
				w.Header().Add("Link", fmt.Sprintf("<%s>; rel=next", next.String()))
			}
		}
		tlJSON, err := json.Marshal(tl)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			s.log.Warn("failed to marshal tag list", "err", err)
			return
		}
		w.Header().Add("Content-Length", fmt.Sprintf("%d", len(tlJSON)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, err = w.Write(tlJSON)
			if err != nil {
				s.log.Warn("failed to marshal tag list", "err", err)
			}
		}
	}
}
