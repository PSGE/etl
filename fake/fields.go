package fake

//========================================================================================
// This file contains code pulled from bigquery golang libraries, to support emulating the
// Uploader function.
//========================================================================================
import (
	"bytes"
	"reflect"
	"sort"
)

// A Field records information about a struct field.
type Field struct {
	Name        string       // effective field name
	NameFromTag bool         // did Name come from a tag?
	Type        reflect.Type // field type
	Index       []int        // index sequence, for reflect.Value.FieldByIndex
	ParsedTag   interface{}  // third return value of the parseTag function

	nameBytes []byte
	equalFold func(s, t []byte) bool
}

type ParseTagFunc func(reflect.StructTag) (name string, keep bool, other interface{}, err error)

type ValidateFunc func(reflect.Type) error

type LeafTypesFunc func(reflect.Type) bool

// A Cache records information about the fields of struct types.
//
// A Cache is safe for use by multiple goroutines.
type FieldCache struct {
	parseTag  ParseTagFunc
	validate  ValidateFunc
	leafTypes LeafTypesFunc
	cache     Cache // from reflect.Type to cacheValue
}

// NewCache constructs a Cache.
//
// Its first argument should be a function that accepts
// a struct tag and returns four values: an alternative name for the field
// extracted from the tag, a boolean saying whether to keep the field or ignore
// it, additional data that is stored with the field information to avoid
// having to parse the tag again, and an error.
//
// Its second argument should be a function that accepts a reflect.Type and
// returns an error if the struct type is invalid in any way. For example, it
// may check that all of the struct field tags are valid, or that all fields
// are of an appropriate type.
func NewFieldCache(parseTag ParseTagFunc, validate ValidateFunc, leafTypes LeafTypesFunc) *FieldCache {
	if parseTag == nil {
		parseTag = func(reflect.StructTag) (string, bool, interface{}, error) {
			return "", true, nil, nil
		}
	}
	if validate == nil {
		validate = func(reflect.Type) error {
			return nil
		}
	}
	if leafTypes == nil {
		leafTypes = func(reflect.Type) bool {
			return false
		}
	}

	return &FieldCache{
		parseTag:  parseTag,
		validate:  validate,
		leafTypes: leafTypes,
	}
}

// A fieldScan represents an item on the fieldByNameFunc scan work list.
type fieldScan struct {
	typ   reflect.Type
	index []int
}

// Fields returns all the exported fields of t, which must be a struct type. It
// follows the standard Go rules for embedded fields, modified by the presence
// of tags. The result is sorted lexicographically by index.
//
// These rules apply in the absence of tags:
// Anonymous struct fields are treated as if their inner exported fields were
// fields in the outer struct (embedding). The result includes all fields that
// aren't shadowed by fields at higher level of embedding. If more than one
// field with the same name exists at the same level of embedding, it is
// excluded. An anonymous field that is not of struct type is treated as having
// its type as its name.
//
// Tags modify these rules as follows:
// A field's tag is used as its name.
// An anonymous struct field with a name given in its tag is treated as
// a field having that name, rather than an embedded struct (the struct's
// fields will not be returned).
// If more than one field with the same name exists at the same level of embedding,
// but exactly one of them is tagged, then the tagged field is reported and the others
// are ignored.
func (c *FieldCache) Fields(t reflect.Type) (List, error) {
	if t.Kind() != reflect.Struct {
		panic("fields: Fields of non-struct type")
	}
	return c.cachedTypeFields(t)
}

// A List is a list of Fields.
type List []Field

// Match returns the field in the list whose name best matches the supplied
// name, nor nil if no field does. If there is a field with the exact name, it
// is returned. Otherwise the first field (sorted by index) whose name matches
// case-insensitively is returned.
func (l List) Match(name string) *Field {
	return l.MatchBytes([]byte(name))
}

// MatchBytes is identical to Match, except that the argument is a byte slice.
func (l List) MatchBytes(name []byte) *Field {
	var f *Field
	for i := range l {
		ff := &l[i]
		if bytes.Equal(ff.nameBytes, name) {
			return ff
		}
		if f == nil && ff.equalFold(ff.nameBytes, name) {
			f = ff
		}
	}
	return f
}

type cacheValue struct {
	fields List
	err    error
}

// cachedTypeFields is like typeFields but uses a cache to avoid repeated work.
// This code has been copied and modified from
// https://go.googlesource.com/go/+/go1.7.3/src/encoding/json/encode.go.
func (c *FieldCache) cachedTypeFields(t reflect.Type) (List, error) {
	cv := c.cache.Get(t, func() interface{} {
		if err := c.validate(t); err != nil {
			return cacheValue{nil, err}
		}
		f, err := c.typeFields(t)
		return cacheValue{List(f), err}
	}).(cacheValue)
	return cv.fields, cv.err
}

func (c *FieldCache) typeFields(t reflect.Type) ([]Field, error) {
	fields, err := c.listFields(t)
	if err != nil {
		return nil, err
	}
	sort.Sort(byName(fields))
	// Delete all fields that are hidden by the Go rules for embedded fields.

	// The fields are sorted in primary order of name, secondary order of field
	// index length. So the first field with a given name is the dominant one.
	var out []Field
	for advance, i := 0, 0; i < len(fields); i += advance {
		// One iteration per name.
		// Find the sequence of fields with the name of this first field.
		fi := fields[i]
		name := fi.Name
		for advance = 1; i+advance < len(fields); advance++ {
			fj := fields[i+advance]
			if fj.Name != name {
				break
			}
		}
		// Find the dominant field, if any, out of all fields that have the same name.
		dominant, ok := dominantField(fields[i : i+advance])
		if ok {
			out = append(out, dominant)
		}
	}
	sort.Sort(byIndex(out))
	return out, nil
}

func (c *FieldCache) listFields(t reflect.Type) ([]Field, error) {
	// This uses the same condition that the Go language does: there must be a unique instance
	// of the match at a given depth level. If there are multiple instances of a match at the
	// same depth, they annihilate each other and inhibit any possible match at a lower level.
	// The algorithm is breadth first search, one depth level at a time.

	// The current and next slices are work queues:
	// current lists the fields to visit on this depth level,
	// and next lists the fields on the next lower level.
	current := []fieldScan{}
	next := []fieldScan{{typ: t}}

	// nextCount records the number of times an embedded type has been
	// encountered and considered for queueing in the 'next' slice.
	// We only queue the first one, but we increment the count on each.
	// If a struct type T can be reached more than once at a given depth level,
	// then it annihilates itself and need not be considered at all when we
	// process that next depth level.
	var nextCount map[reflect.Type]int

	// visited records the structs that have been considered already.
	// Embedded pointer fields can create cycles in the graph of
	// reachable embedded types; visited avoids following those cycles.
	// It also avoids duplicated effort: if we didn't find the field in an
	// embedded type T at level 2, we won't find it in one at level 4 either.
	visited := map[reflect.Type]bool{}

	var fields []Field // Fields found.

	for len(next) > 0 {
		current, next = next, current[:0]
		count := nextCount
		nextCount = nil

		// Process all the fields at this depth, now listed in 'current'.
		// The loop queues embedded fields found in 'next', for processing during the next
		// iteration. The multiplicity of the 'current' field counts is recorded
		// in 'count'; the multiplicity of the 'next' field counts is recorded in 'nextCount'.
		for _, scan := range current {
			t := scan.typ
			if visited[t] {
				// We've looked through this type before, at a higher level.
				// That higher level would shadow the lower level we're now at,
				// so this one can't be useful to us. Ignore it.
				continue
			}
			visited[t] = true
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)

				exported := (f.PkgPath == "")

				// If a named field is unexported, ignore it. An anonymous
				// unexported field is processed, because it may contain
				// exported fields, which are visible.
				if !exported && !f.Anonymous {
					continue
				}

				// Examine the tag.
				tagName, keep, other, err := c.parseTag(f.Tag)
				if err != nil {
					return nil, err
				}
				if !keep {
					continue
				}
				if c.leafTypes(f.Type) {
					fields = append(fields, newField(f, tagName, other, scan.index, i))
					continue
				}

				var ntyp reflect.Type
				if f.Anonymous {
					// Anonymous field of type T or *T.
					ntyp = f.Type
					if ntyp.Kind() == reflect.Ptr {
						ntyp = ntyp.Elem()
					}
				}

				// Record fields with a tag name, non-anonymous fields, or
				// anonymous non-struct fields.
				if tagName != "" || ntyp == nil || ntyp.Kind() != reflect.Struct {
					if !exported {
						continue
					}
					fields = append(fields, newField(f, tagName, other, scan.index, i))
					if count[t] > 1 {
						// If there were multiple instances, add a second,
						// so that the annihilation code will see a duplicate.
						fields = append(fields, fields[len(fields)-1])
					}
					continue
				}

				// Queue embedded struct fields for processing with next level,
				// but only if the embedded types haven't already been queued.
				if nextCount[ntyp] > 0 {
					nextCount[ntyp] = 2 // exact multiple doesn't matter
					continue
				}
				if nextCount == nil {
					nextCount = map[reflect.Type]int{}
				}
				nextCount[ntyp] = 1
				if count[t] > 1 {
					nextCount[ntyp] = 2 // exact multiple doesn't matter
				}
				var index []int
				index = append(index, scan.index...)
				index = append(index, i)
				next = append(next, fieldScan{ntyp, index})
			}
		}
	}
	return fields, nil
}

func newField(f reflect.StructField, tagName string, other interface{}, index []int, i int) Field {
	name := tagName
	if name == "" {
		name = f.Name
	}
	sf := Field{
		Name:        name,
		NameFromTag: tagName != "",
		Type:        f.Type,
		ParsedTag:   other,
		nameBytes:   []byte(name),
	}
	sf.equalFold = foldFunc(sf.nameBytes)
	sf.Index = append(sf.Index, index...)
	sf.Index = append(sf.Index, i)
	return sf
}

// byName sorts fields using the following criteria, in order:
// 1. name
// 2. embedding depth
// 3. tag presence (preferring a tagged field)
// 4. index sequence.
type byName []Field

func (x byName) Len() int { return len(x) }

func (x byName) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byName) Less(i, j int) bool {
	if x[i].Name != x[j].Name {
		return x[i].Name < x[j].Name
	}
	if len(x[i].Index) != len(x[j].Index) {
		return len(x[i].Index) < len(x[j].Index)
	}
	if x[i].NameFromTag != x[j].NameFromTag {
		return x[i].NameFromTag
	}
	return byIndex(x).Less(i, j)
}

// byIndex sorts field by index sequence.
type byIndex []Field

func (x byIndex) Len() int { return len(x) }

func (x byIndex) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byIndex) Less(i, j int) bool {
	xi := x[i].Index
	xj := x[j].Index
	ln := len(xi)
	if l := len(xj); l < ln {
		ln = l
	}
	for k := 0; k < ln; k++ {
		if xi[k] != xj[k] {
			return xi[k] < xj[k]
		}
	}
	return len(xi) < len(xj)
}

// dominantField looks through the fields, all of which are known to have the
// same name, to find the single field that dominates the others using Go's
// embedding rules, modified by the presence of tags. If there are multiple
// top-level fields, the boolean will be false: This condition is an error in
// Go and we skip all the fields.
func dominantField(fs []Field) (Field, bool) {
	// The fields are sorted in increasing index-length order, then by presence of tag.
	// That means that the first field is the dominant one. We need only check
	// for error cases: two fields at top level, either both tagged or neither tagged.
	if len(fs) > 1 && len(fs[0].Index) == len(fs[1].Index) && fs[0].NameFromTag == fs[1].NameFromTag {
		return Field{}, false
	}
	return fs[0], true
}
