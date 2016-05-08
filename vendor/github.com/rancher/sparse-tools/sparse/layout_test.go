package sparse

import (
	"os"
	"reflect"
	"testing"

	"github.com/rancher/sparse-tools/log"
)

var name = tempFilePath("")

func TestLayout0(t *testing.T) {
	layoutModel := []FileInterval{}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout1(t *testing.T) {
	layoutModel := []FileInterval{{SparseHole, Interval{0, 4 * Blocks}}}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout2(t *testing.T) {
	layoutModel := []FileInterval{{SparseData, Interval{0, 4 * Blocks}}}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout3(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseHole, Interval{0, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 4 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout4(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 4 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout5(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout6(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout7(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutExpected := []FileInterval{
		{SparseData, Interval{0, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutExpected)
}

func TestLayout8(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutExpected := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 3 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutExpected)
}

func TestLayout9(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 256 * Blocks}},
		{SparseHole, Interval{256 * Blocks, (256<<10 - 256) * Blocks}},
		{SparseData, Interval{(256<<10 - 256) * Blocks, 256 << 10 * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout10(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseHole, Interval{0, (256<<10 - 128) * Blocks}},
		{SparseData, Interval{(256<<10 - 128) * Blocks, (256<<10 - 8) * Blocks}},
		{SparseHole, Interval{(256<<10 - 8) * Blocks, (256<<10 + 8) * Blocks}},
		{SparseData, Interval{(256<<10 + 8) * Blocks, (256<<10 + 128) * Blocks}},
		{SparseHole, Interval{(256<<10 + 128) * Blocks, (256 << 10 * 2) * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestLayout11(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseHole, Interval{0, (256<<10 - 8) * Blocks}},
		{SparseData, Interval{(256<<10 - 8) * Blocks, (256<<10 + 8) * Blocks}},
		{SparseHole, Interval{(256<<10 + 8) * Blocks, (256 << 10 * 2) * Blocks}},
	}
	layoutTest(t, name, layoutModel, layoutModel)
}

func TestPunchHole0(t *testing.T) {
	layoutModel := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutExpected := []FileInterval{
		{SparseHole, Interval{0 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	punchHoleTest(t, name, layoutModel, Interval{0, 1 * Blocks}, layoutExpected)
}

func layoutTest(t *testing.T, name string, layoutModel, layoutExpected []FileInterval) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	defer fileCleanup(name)
	createTestSparseFile(name, layoutModel)

	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		t.Fatal(err)
	}

	layoutActual, err := RetrieveLayout(f, Interval{0, size})
	if err != nil || !reflect.DeepEqual(layoutExpected, layoutActual) {
		t.Fatal("wrong sparse layout")
	}

	if checkTestSparseFile(name, layoutModel) != nil {
		t.Fatal("wrong sparse layout content")
	}
}

func punchHoleTest(t *testing.T, name string, layoutModel []FileInterval, hole Interval, layoutExpected []FileInterval) {
	createTestSparseFile(name, layoutModel)

	defer fileCleanup(name)
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		t.Fatal(err)
	}

	err = PunchHole(f, hole)
	if err != nil {
		t.Fatal(err)
	}

	layoutActual, err := RetrieveLayout(f, Interval{0, size})
	if err != nil || !reflect.DeepEqual(layoutExpected, layoutActual) {
		t.Fatal("wrong sparse layout")
	}
}

func makeData(interval FileInterval) []byte {
	data := make([]byte, interval.Len())
	if SparseData == interval.Kind {
		for i := range data {
			data[i] = byte(interval.Begin/Blocks + 1)
		}
	}
	return data
}
