package memorytable

import (
	"testing"
)

func TestSkipTable_Set_Get(t *testing.T) {
	st := NewMemoryTable()

	// 插入数据
	st.Set("key1", []byte("value1"), false)
	st.Set("key2", []byte("value2"), false)

	// 查询数据
	value := st.Get("key1")
	if string(value) != "value1" {
		t.Errorf("Expected value 'value1', but got %s", value)
	}

	value = st.Get("key2")
	if string(value) != "value2" {
		t.Errorf("Expected value 'value2', but got %s", value)
	}
}

func TestSkipTable_Set_Update(t *testing.T) {
	st := NewMemoryTable()

	// 插入数据
	st.Set("key1", []byte("value1"), false)

	// 更新数据
	st.Set("key1", []byte("updated_value1"), false)

	// 查询更新后的数据
	value := st.Get("key1")
	if string(value) != "updated_value1" {
		t.Errorf("Expected updated value 'updated_value1', but got %s", value)
	}
}

func TestSkipTable_Set_Deleted(t *testing.T) {
	st := NewMemoryTable()

	// 插入数据
	st.Set("key1", []byte("value1"), false)

	// 删除数据（标记 Deleted 为 true）
	st.Set("key1", []byte("value1"), true)

	// 查询已删除的数据
	value := st.Get("key1")
	if value != nil {
		t.Errorf("Expected nil value for deleted key, but got %s", value)
	}
}

func TestSkipTable_Size(t *testing.T) {
	st := NewMemoryTable()

	// 插入数据
	st.Set("key1", []byte("value1"), false)
	st.Set("key2", []byte("value2"), false)

	// 检查内存表大小
	size := st.Size()
	expectedSize := int64(len("key1") + len("value1") + len("key2") + len("value2"))
	if size != expectedSize {
		t.Errorf("Expected size %d, but got %d", expectedSize, size)
	}
}

func TestSkipTable_Scan(t *testing.T) {
	st := NewMemoryTable()

	// 插入数据
	st.Set("key1", []byte("value1"), false)
	st.Set("key2", []byte("value2"), false)
	st.Set("key3", []byte("value3"), false)

	// 执行扫描
	if !st.Scan() {
		t.Errorf("Scan failed on the first node")
	}

	// 扫描第一个值
	chunk := st.ScanValue()
	if chunk.Key != "key1" || string(chunk.Value) != "value1" {
		t.Errorf("Expected key1 with value1, but got %s with %s", chunk.Key, chunk.Value)
	}

	// 执行扫描到下一个节点
	if !st.Scan() {
		t.Errorf("Scan failed on the second node")
	}

	// 扫描第二个值
	chunk = st.ScanValue()
	if chunk.Key != "key2" || string(chunk.Value) != "value2" {
		t.Errorf("Expected key2 with value2, but got %s with %s", chunk.Key, chunk.Value)
	}
}

func TestSkipTable_Scan_End(t *testing.T) {
	st := NewMemoryTable()

	// 插入数据
	st.Set("key1", []byte("value1"), false)

	// 执行扫描
	if !st.Scan() {
		t.Errorf("Scan failed on the first node")
	}

	// 扫描第一个值
	chunk := st.ScanValue()
	if chunk.Key != "key1" || string(chunk.Value) != "value1" {
		t.Errorf("Expected key1 with value1, but got %s with %s", chunk.Key, chunk.Value)
	}

	// 再次扫描时应返回 false，表示没有更多数据
	if st.Scan() {
		t.Errorf("Scan should return false at the end")
	}
}
