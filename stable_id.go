package lazily

// Manufactured identity for text — stable-id alignment.
//
// Three layers of identity manufacturing:
//  1. In-band anchors (exact, survive body rewrite).
//  2. Content-derived hashes of whitespace-normalized text (survive
//     reflow/reorder, change on edit).
//  3. Word-LCS similarity alignment (>= 0.5 => Edited/key-inherited; below =>
//     Inserted).
//
// Keys carry an "a:"/"c:" prefix so anchored and content keyspaces never
// collide. Mirrors lazily-js/src/stable-id.js and lazily-dart
// lib/src/stable_id.dart. Conforms to lazily-spec
// conformance/collections/stableid_alignment.json.

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf16"
)

// EditThreshold is the edit-similarity threshold below which a match is treated
// as an insert.
const EditThreshold = 0.5

// AnchorPrefix is the anchored-key wire prefix.
const AnchorPrefix = "a:"

// ContentPrefix is the content-key wire prefix.
const ContentPrefix = "c:"

// whitespaceRe splits on runs of whitespace (mirrors the Dart/JS \s+ regex).
var whitespaceRe = regexp.MustCompile(`\s+`)

// Block is a text block, optionally anchored.
type Block struct {
	Text   string
	Anchor *string // nil when the block is not anchored
}

// NewBlock constructs an unanchored text block (Dart Block.text).
func NewBlock(text string) Block { return Block{Text: text} }

// NewAnchoredBlock constructs an anchored block (Dart Block.anchored).
func NewAnchoredBlock(anchor, text string) Block {
	return Block{Text: text, Anchor: &anchor}
}

func (b Block) String() string {
	if b.Anchor != nil {
		return fmt.Sprintf("Block(%s:%s)", *b.Anchor, b.Text)
	}
	return fmt.Sprintf("Block(%s)", b.Text)
}

// BlockKey is a manufactured block key: either anchored or content-derived.
// Anchored keys carry a string value; content keys carry a 64-bit FNV-1a hash.
type BlockKey struct {
	Kind         string // "anchored" | "content"
	AnchorValue  string // valid when Kind == "anchored"
	ContentValue uint64 // valid when Kind == "content"
	isContent    bool
}

// AnchoredBlockKey constructs an anchored key (Dart BlockKey.anchored).
func AnchoredBlockKey(value string) BlockKey {
	return BlockKey{Kind: "anchored", AnchorValue: value, isContent: false}
}

// ContentBlockKey constructs a content-derived key (Dart BlockKey.content).
func ContentBlockKey(value uint64) BlockKey {
	return BlockKey{Kind: "content", ContentValue: value, isContent: true}
}

// IsAnchored reports whether this is an anchored key.
func (k BlockKey) IsAnchored() bool { return !k.isContent }

// IsContent reports whether this is a content-derived key.
func (k BlockKey) IsContent() bool { return k.isContent }

// Equals reports structural equality with other.
func (k BlockKey) Equals(other BlockKey) bool {
	if k.Kind != other.Kind {
		return false
	}
	if k.isContent {
		return k.ContentValue == other.ContentValue
	}
	return k.AnchorValue == other.AnchorValue
}

// AsString renders the wire form: "a:<anchor>" or "c:" + 16-char zero-padded
// hex of the 64-bit content hash.
func (k BlockKey) AsString() string {
	if k.IsAnchored() {
		return AnchorPrefix + k.AnchorValue
	}
	return fmt.Sprintf("%s%016x", ContentPrefix, k.ContentValue)
}

func (k BlockKey) String() string { return k.AsString() }

// Normalize collapses whitespace: split on \s+, drop empties, join with a
// single space.
func Normalize(text string) string {
	parts := whitespaceRe.Split(text, -1)
	kept := parts[:0]
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, " ")
}

// ContentHash computes the FNV-1a 64-bit content hash of the UTF-8 bytes of
// Normalize(text). Cross-language stable (NOT Go's hash/fnv is equivalent, but
// the byte source mirrors the Dart code-unit encoding exactly).
func ContentHash(text string) uint64 {
	const offset uint64 = 0xcbf29ce484222325
	const prime uint64 = 0x100000001b3
	hash := offset
	for _, b := range utf8Bytes(Normalize(text)) {
		hash ^= uint64(b)
		hash *= prime
	}
	return hash
}

// utf8Bytes encodes s the same way the Dart source does: it iterates UTF-16
// code units and emits 1/2/3-byte sequences. This matches Dart's codeUnits
// loop (including its surrogate handling) byte-for-byte so the FNV hash agrees
// across languages.
func utf8Bytes(s string) []byte {
	units := utf16.Encode([]rune(s))
	out := make([]byte, 0, len(units))
	for _, r := range units {
		switch {
		case r < 0x80:
			out = append(out, byte(r))
		case r < 0x800:
			out = append(out, byte(0xC0|(r>>6)))
			out = append(out, byte(0x80|(r&0x3F)))
		default:
			out = append(out, byte(0xE0|(r>>12)))
			out = append(out, byte(0x80|((r>>6)&0x3F)))
			out = append(out, byte(0x80|(r&0x3F)))
		}
	}
	return out
}

// BlockKeyOf computes the manufactured key for block: anchor wins, else content
// hash (Dart blockKey).
func BlockKeyOf(block Block) BlockKey {
	if block.Anchor != nil {
		return AnchoredBlockKey(*block.Anchor)
	}
	return ContentBlockKey(ContentHash(block.Text))
}

// lcsLen computes the word-LCS length via rolling two-row DP.
func lcsLen(a, b []string) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				curr[j] = prev[j-1] + 1
			} else if prev[j] > curr[j-1] {
				curr[j] = prev[j]
			} else {
				curr[j] = curr[j-1]
			}
		}
		prev, curr = curr, prev
		for j := 0; j <= len(b); j++ {
			curr[j] = 0
		}
	}
	return prev[len(b)]
}

func tokenize(s string) []string {
	parts := whitespaceRe.Split(s, -1)
	out := parts[:0]
	for _, t := range parts {
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

// Similarity returns a value in [0, 1]: 2*|word-LCS| / (|a| + |b|). Both-empty
// is 1.0; exactly-one-empty is 0.0.
func Similarity(a, b string) float64 {
	ta := tokenize(a)
	tb := tokenize(b)
	if len(ta) == 0 && len(tb) == 0 {
		return 1.0
	}
	if len(ta) == 0 || len(tb) == 0 {
		return 0.0
	}
	lcs := lcsLen(ta, tb)
	return float64(2*lcs) / float64(len(ta)+len(tb))
}

// Match is the kind of match for a new block against the old set.
type Match struct {
	Kind       string // "same" | "edited" | "inserted"
	OldIndex   int    // -1 for inserted
	Similarity float64
}

// MatchSame constructs a Same match.
func MatchSame(oldIndex int) Match {
	return Match{Kind: "same", OldIndex: oldIndex, Similarity: 1.0}
}

// MatchEdited constructs an Edited match.
func MatchEdited(oldIndex int, similarity float64) Match {
	return Match{Kind: "edited", OldIndex: oldIndex, Similarity: similarity}
}

// MatchInserted constructs an Inserted match.
func MatchInserted() Match {
	return Match{Kind: "inserted", OldIndex: -1, Similarity: 0.0}
}

func (m Match) String() string {
	if m.Kind == "inserted" {
		return "Inserted"
	}
	return fmt.Sprintf("%s%s:%d", strings.ToUpper(m.Kind[:1]), m.Kind[1:], m.OldIndex)
}

// Alignment is the alignment of new blocks against old, plus the set of removed
// old indices.
type Alignment struct {
	NewMatches []Match // one per new block
	Removed    []int   // old indices not matched
}

func (a Alignment) String() string {
	strs := make([]string, len(a.NewMatches))
	for i, m := range a.NewMatches {
		strs[i] = m.String()
	}
	return fmt.Sprintf("Alignment(matches=(%s), removed=%v)", strings.Join(strs, ", "), a.Removed)
}

// Align aligns newBlocks against oldBlocks: exact-key match first, then
// similarity (>= threshold) with nearest-index tiebreak.
func Align(oldBlocks, newBlocks []Block) Alignment {
	oldKeys := make([]BlockKey, len(oldBlocks))
	for i, b := range oldBlocks {
		oldKeys[i] = BlockKeyOf(b)
	}
	newKeys := make([]BlockKey, len(newBlocks))
	for i, b := range newBlocks {
		newKeys[i] = BlockKeyOf(b)
	}
	oldUsed := make([]bool, len(oldBlocks))
	matches := make([]Match, len(newBlocks))
	matched := make([]bool, len(newBlocks))

	// Pass 1: exact key match (lowest unused old index).
	for ni := 0; ni < len(newBlocks); ni++ {
		for oi := 0; oi < len(oldBlocks); oi++ {
			if !oldUsed[oi] && newKeys[ni].Equals(oldKeys[oi]) {
				matches[ni] = MatchSame(oi)
				matched[ni] = true
				oldUsed[oi] = true
				break
			}
		}
	}

	// Pass 2: similarity match for unmatched new blocks.
	for ni := 0; ni < len(newBlocks); ni++ {
		if matched[ni] {
			continue
		}
		bestOi := -1
		bestSim := 0.0
		bestDist := int(^uint(0) >> 1) // max int
		for oi := 0; oi < len(oldBlocks); oi++ {
			if oldUsed[oi] {
				continue
			}
			sim := Similarity(newBlocks[ni].Text, oldBlocks[oi].Text)
			dist := oi - ni
			if dist < 0 {
				dist = -dist
			}
			if sim > bestSim || (sim == bestSim && sim >= EditThreshold && dist < bestDist) {
				bestSim = sim
				bestOi = oi
				bestDist = dist
			}
		}
		if bestOi >= 0 && bestSim >= EditThreshold {
			matches[ni] = MatchEdited(bestOi, bestSim)
			oldUsed[bestOi] = true
		} else {
			matches[ni] = MatchInserted()
		}
		matched[ni] = true
	}

	removed := []int{}
	for oi := 0; oi < len(oldBlocks); oi++ {
		if !oldUsed[oi] {
			removed = append(removed, oi)
		}
	}

	return Alignment{NewMatches: matches, Removed: removed}
}

// AssignStableKeys assigns stable keys to newBlocks by flowing identity through
// the alignment with oldBlocks. Same/Edited inherit the predecessor's key;
// Inserted get a fresh key.
func AssignStableKeys(oldBlocks, newBlocks []Block) []string {
	alignment := Align(oldBlocks, newBlocks)
	result := make([]string, 0, len(newBlocks))
	for ni := 0; ni < len(newBlocks); ni++ {
		m := alignment.NewMatches[ni]
		if m.Kind == "inserted" {
			result = append(result, BlockKeyOf(newBlocks[ni]).AsString())
		} else {
			result = append(result, BlockKeyOf(oldBlocks[m.OldIndex]).AsString())
		}
	}
	return result
}
