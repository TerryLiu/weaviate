//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package byteops 提供了用于在缓冲区中序列化和反序列化对象的辅助函数
// 主要功能包括：
// - 基本数据类型的读写（uint64, uint32, uint16, uint8）
// - 字节数组的操作
// - 浮点数数组与字节的相互转换
// - 整数数组与字节的相互转换
// - 支持带长度指示器的数据读写
package byteops

import (
	"encoding/binary" // 用于二进制数据的编解码
	"errors"          // 用于错误处理
	"io"              // 用于实现 io.Writer 接口
	"math"            // 用于浮点数位操作
)

// 定义各种整数类型占用的字节数常量
const (
	Uint64Len = 8  // uint64 类型占用的字节数
	Uint32Len = 4  // uint32 类型占用的字节数
	Uint16Len = 2  // uint16 类型占用的字节数
	Uint8Len  = 1  // uint8 类型占用的字节数
)

// ReadWriter 是一个用于在字节缓冲区上进行读写操作的结构体
// 包含当前位置指针和底层字节缓冲区
type ReadWriter struct {
	Position uint64  // 当前读写位置
	Buffer   []byte  // 底层字节缓冲区
}

// WithPosition 返回一个设置初始位置的选项函数
// 用于在创建 ReadWriter 时指定起始位置
func WithPosition(pos uint64) func(*ReadWriter) {
	return func(rw *ReadWriter) {
		rw.Position = pos
	}
}

// NewReadWriter 创建一个新的 ReadWriter 实例
// 参数 buf: 底层字节缓冲区
func NewReadWriter(buf []byte) ReadWriter {
	rw := ReadWriter{Buffer: buf}
	return rw
}

// NewReadWriterWithOps 创建带有选项配置的 ReadWriter 实例
// 即使没有提供选项也会逃逸到堆上分配内存
// 参数 buf: 底层字节缓冲区
// 参数 opts: 可变的配置选项函数
func NewReadWriterWithOps(buf []byte, opts ...func(writer *ReadWriter)) ReadWriter {
	rw := ReadWriter{Buffer: buf}
	for _, opt := range opts {
		opt(&rw)
	}
	return rw
}

// ResetBuffer 重置缓冲区和位置
// 参数 buf: 新的字节缓冲区
func (bo *ReadWriter) ResetBuffer(buf []byte) {
	bo.Buffer = buf
	bo.Position = 0
}

// ReadUint64 从当前位置读取一个 uint64 值（小端序）
// 并将位置向前移动 8 个字节
func (bo *ReadWriter) ReadUint64() uint64 {
	bo.Position += Uint64Len
	return binary.LittleEndian.Uint64(bo.Buffer[bo.Position-Uint64Len : bo.Position])
}

// ReadUint16 从当前位置读取一个 uint16 值（小端序）
// 并将位置向前移动 2 个字节
func (bo *ReadWriter) ReadUint16() uint16 {
	bo.Position += Uint16Len
	return binary.LittleEndian.Uint16(bo.Buffer[bo.Position-Uint16Len : bo.Position])
}

// ReadUint32 从当前位置读取一个 uint32 值（小端序）
// 并将位置向前移动 4 个字节
func (bo *ReadWriter) ReadUint32() uint32 {
	bo.Position += Uint32Len
	return binary.LittleEndian.Uint32(bo.Buffer[bo.Position-Uint32Len : bo.Position])
}

// ReadUint8 从当前位置读取一个 uint8 值
// 并将位置向前移动 1 个字节
func (bo *ReadWriter) ReadUint8() uint8 {
	bo.Position += Uint8Len
	return bo.Buffer[bo.Position-Uint8Len]
}

// CopyBytesFromBuffer 从缓冲区复制指定长度的字节到输出切片
// 如果输出切片为 nil，则会创建新的切片
// 参数 length: 要复制的字节数
// 参数 out: 输出字节切片（可为 nil）
// 返回值: 复制后的字节切片和可能的错误
func (bo *ReadWriter) CopyBytesFromBuffer(length uint64, out []byte) ([]byte, error) {
	if out == nil {
		out = make([]byte, length)
	}
	bo.Position += length
	numCopiedBytes := copy(out, bo.Buffer[bo.Position-length:bo.Position])
	if numCopiedBytes != int(length) {
		return nil, errors.New("could not copy data from buffer")
	}
	return out, nil
}

// ReadBytesFromBuffer 从缓冲区读取指定长度的字节切片（不复制）
// 直接返回缓冲区的子切片引用
// 参数 length: 要读取的字节数
// 返回值: 缓冲区的子切片
func (bo *ReadWriter) ReadBytesFromBuffer(length uint64) []byte {
	subslice := bo.Buffer[bo.Position : bo.Position+length]
	bo.Position += length
	return subslice
}

// ReadBytesFromBufferWithUint64LengthIndicator 读取带有 uint64 长度指示器的字节数据
// 先读取 8 字节的长度信息，然后读取相应长度的数据
// 返回值: 实际的数据字节切片
func (bo *ReadWriter) ReadBytesFromBufferWithUint64LengthIndicator() []byte {
	bo.Position += Uint64Len
	bufLen := binary.LittleEndian.Uint64(bo.Buffer[bo.Position-Uint64Len : bo.Position])

	bo.Position += bufLen
	subslice := bo.Buffer[bo.Position-bufLen : bo.Position]
	return subslice
}

// DiscardBytesFromBufferWithUint64LengthIndicator 丢弃带有 uint64 长度指示器的数据
// 只读取长度信息并跳过相应的数据，不实际复制数据
// 返回值: 被丢弃的数据长度
func (bo *ReadWriter) DiscardBytesFromBufferWithUint64LengthIndicator() uint64 {
	bo.Position += Uint64Len
	bufLen := binary.LittleEndian.Uint64(bo.Buffer[bo.Position-Uint64Len : bo.Position])

	bo.Position += bufLen
	return bufLen
}

// ReadBytesFromBufferWithUint32LengthIndicator 读取带有 uint32 长度指示器的字节数据
// 先读取 4 字节的长度信息，然后读取相应长度的数据
// 返回值: 实际的数据字节切片
func (bo *ReadWriter) ReadBytesFromBufferWithUint32LengthIndicator() []byte {
	bo.Position += Uint32Len
	bufLen := uint64(binary.LittleEndian.Uint32(bo.Buffer[bo.Position-Uint32Len : bo.Position]))

	bo.Position += bufLen
	subslice := bo.Buffer[bo.Position-bufLen : bo.Position]
	return subslice
}

// DiscardBytesFromBufferWithUint32LengthIndicator 丢弃带有 uint32 长度指示器的数据
// 只读取长度信息并跳过相应的数据，不实际复制数据
// 返回值: 被丢弃的数据长度
func (bo *ReadWriter) DiscardBytesFromBufferWithUint32LengthIndicator() uint32 {
	bo.Position += Uint32Len
	bufLen := binary.LittleEndian.Uint32(bo.Buffer[bo.Position-Uint32Len : bo.Position])

	bo.Position += uint64(bufLen)
	return bufLen
}

// WriteUint64 向当前位置写入一个 uint64 值（小端序）
// 并将位置向前移动 8 个字节
// 参数 value: 要写入的 uint64 值
func (bo *ReadWriter) WriteUint64(value uint64) {
	bo.Position += Uint64Len
	binary.LittleEndian.PutUint64(bo.Buffer[bo.Position-Uint64Len:bo.Position], value)
}

// WriteUint32 向当前位置写入一个 uint32 值（小端序）
// 并将位置向前移动 4 个字节
// 参数 value: 要写入的 uint32 值
func (bo *ReadWriter) WriteUint32(value uint32) {
	bo.Position += Uint32Len
	binary.LittleEndian.PutUint32(bo.Buffer[bo.Position-Uint32Len:bo.Position], value)
}

// WriteUint16 向当前位置写入一个 uint16 值（小端序）
// 并将位置向前移动 2 个字节
// 参数 value: 要写入的 uint16 值
func (bo *ReadWriter) WriteUint16(value uint16) {
	bo.Position += Uint16Len
	binary.LittleEndian.PutUint16(bo.Buffer[bo.Position-Uint16Len:bo.Position], value)
}

// CopyBytesToBuffer 将字节切片复制到缓冲区的当前位置
// 参数 copyBytes: 要复制的字节切片
// 返回值: 可能的错误信息
func (bo *ReadWriter) CopyBytesToBuffer(copyBytes []byte) error {
	lenCopyBytes := uint64(len(copyBytes))
	bo.Position += lenCopyBytes
	numCopiedBytes := copy(bo.Buffer[bo.Position-lenCopyBytes:bo.Position], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	return nil
}

// Write 实现 io.Writer 接口
// 将字节切片写入缓冲区
// 参数 p: 要写入的字节切片
// 返回值: 实际写入的字节数和可能的错误
func (bo *ReadWriter) Write(p []byte) (int, error) {
	lenCopyBytes := uint64(len(p))
	bo.Position += lenCopyBytes
	if bo.Position > uint64(len(bo.Buffer)) {
		return 0, io.EOF
	}
	numCopiedBytes := copy(bo.Buffer[bo.Position-lenCopyBytes:bo.Position], p)
	return numCopiedBytes, nil
}

// CopyBytesToBufferWithUint64LengthIndicator 将字节切片写入缓冲区，并在前面添加 uint64 长度指示器
// 先写入 8 字节的长度信息，然后写入实际数据
// 参数 copyBytes: 要写入的字节切片
// 返回值: 可能的错误信息
func (bo *ReadWriter) CopyBytesToBufferWithUint64LengthIndicator(copyBytes []byte) error {
	lenCopyBytes := uint64(len(copyBytes))
	bo.Position += Uint64Len
	binary.LittleEndian.PutUint64(bo.Buffer[bo.Position-Uint64Len:bo.Position], lenCopyBytes)
	bo.Position += lenCopyBytes
	numCopiedBytes := copy(bo.Buffer[bo.Position-lenCopyBytes:bo.Position], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	return nil
}

// CopyBytesToBufferWithUint32LengthIndicator 将字节切片写入缓冲区，并在前面添加 uint32 长度指示器
// 先写入 4 字节的长度信息，然后写入实际数据
// 参数 copyBytes: 要写入的字节切片
// 返回值: 可能的错误信息
func (bo *ReadWriter) CopyBytesToBufferWithUint32LengthIndicator(copyBytes []byte) error {
	lenCopyBytes := uint32(len(copyBytes))
	bo.Position += Uint32Len
	binary.LittleEndian.PutUint32(bo.Buffer[bo.Position-Uint32Len:bo.Position], lenCopyBytes)
	bo.Position += uint64(lenCopyBytes)
	numCopiedBytes := copy(bo.Buffer[bo.Position-uint64(lenCopyBytes):bo.Position], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	return nil
}

// MoveBufferPositionForward 将缓冲区位置向前移动指定长度
// 参数 length: 要移动的字节数
func (bo *ReadWriter) MoveBufferPositionForward(length uint64) {
	bo.Position += length
}

// MoveBufferToAbsolutePosition 将缓冲区位置移动到绝对位置
// 参数 pos: 目标位置
func (bo *ReadWriter) MoveBufferToAbsolutePosition(pos uint64) {
	bo.Position = pos
}

// WriteByte 向当前位置写入单个字节
// 参数 b: 要写入的字节
func (bo *ReadWriter) WriteByte(b byte) {
	bo.Buffer[bo.Position] = b
	bo.Position += 1
}

// Fp32SliceToBytes 将 float32 切片转换为字节切片
// 每个 float32 值占用 4 个字节（小端序）
// 参数 slice: 输入的 float32 切片
// 返回值: 转换后的字节切片
func Fp32SliceToBytes(slice []float32) []byte {
	if len(slice) == 0 {
		return []byte{}
	}
	vector := make([]byte, len(slice)*Uint32Len)
	for i := 0; i < len(slice); i++ {
		binary.LittleEndian.PutUint32(vector[i*Uint32Len:(i+1)*Uint32Len], math.Float32bits(slice[i]))
	}
	return vector
}

// Fp32SliceOfSlicesToBytes 将二维 float32 切片转换为字节切片
//
// 在字节切片中，前两个字节用 uint16 类型编码内部切片的维度数量
// 剩余字节是 float32 值
//
// 如果外层切片为空，返回空字节切片
// 如果第一个内层切片为空，返回空字节切片
// 参数 slices: 二维 float32 切片
// 返回值: 转换后的字节切片
func Fp32SliceOfSlicesToBytes(slices [][]float32) []byte {
	if len(slices) == 0 {
		return []byte{}
	}
	if len(slices[0]) == 0 {
		return []byte{}
	}
	dimensions := len(slices[0])
	// 创建大小为 2 的字节切片，容量为 2 + (切片数量 * 浮点数数量 * 4)
	bytes := make([]byte, Uint16Len, Uint16Len+len(slices)*dimensions*Uint32Len)
	// 将维度数量写入前 2 个字节
	binary.LittleEndian.PutUint16(bytes[:Uint16Len], uint16(dimensions))
	// 循环遍历切片并将它们转换为字节追加到结果中
	for _, slice := range slices {
		bytes = append(bytes, Fp32SliceToBytes(slice)...)
	}
	return bytes
}

// Fp64SliceToBytes 将 float64 切片转换为字节切片
// 每个 float64 值占用 8 个字节（小端序）
// 参数 floats: 输入的 float64 切片
// 返回值: 转换后的字节切片
func Fp64SliceToBytes(floats []float64) []byte {
	vector := make([]byte, len(floats)*Uint64Len)
	for i := 0; i < len(floats); i++ {
		binary.LittleEndian.PutUint64(vector[i*Uint64Len:(i+1)*Uint64Len], math.Float64bits(floats[i]))
	}
	return vector
}

// Fp32SliceFromBytes 将字节切片转换回 float32 切片
// 参数 vector: 输入的字节切片
// 返回值: 转换后的 float32 切片
func Fp32SliceFromBytes(vector []byte) []float32 {
	floats := make([]float32, len(vector)/Uint32Len)
	for i := 0; i < len(floats); i++ {
		asUint := binary.LittleEndian.Uint32(vector[i*Uint32Len : (i+1)*Uint32Len])
		floats[i] = math.Float32frombits(asUint)
	}
	return floats
}

// Fp32SliceOfSlicesFromBytes 将字节切片转换回二维 float32 切片
//
// 在字节切片中，使用前两个字节确定内部切片的维度（推断为 uint16 类型）
// 剩余字节是 float32 值
//
// 如果字节切片为空，返回空的二维切片
// 如果维度被发现为 0 则返回错误，因为这是无效的
// 参数 bytes: 输入的字节切片
// 返回值: 转换后的二维 float32 切片和可能的错误
func Fp32SliceOfSlicesFromBytes(bytes []byte) ([][]float32, error) {
	if len(bytes) == 0 {
		return [][]float32{}, nil
	}
	// 读取前 2 个字节获取内部切片的维度
	dimension := int(binary.LittleEndian.Uint16(bytes[:Uint16Len]))
	if dimension == 0 {
		return nil, errors.New("dimension cannot be 0")
	}
	// 丢弃前 2 个字节
	bytes = bytes[Uint16Len:]
	// 计算有多少个内部切片
	howMany := len(bytes) / (dimension * Uint32Len)
	vectors := make([][]float32, howMany)
	// 循环遍历字节，根据维度提取切片
	for i := 0; i < howMany; i++ {
		vectors[i] = Fp32SliceFromBytes(bytes[i*dimension*Uint32Len : (i+1)*dimension*Uint32Len])
	}
	return vectors, nil
}

// Fp64SliceFromBytes 将字节切片转换回 float64 切片
// 参数 vector: 输入的字节切片
// 返回值: 转换后的 float64 切片
func Fp64SliceFromBytes(vector []byte) []float64 {
	floats := make([]float64, len(vector)/Uint64Len)
	for i := 0; i < len(floats); i++ {
		asUint := binary.LittleEndian.Uint64(vector[i*Uint64Len : (i+1)*Uint64Len])
		floats[i] = math.Float64frombits(asUint)
	}
	return floats
}

// IntsToByteVector 将 float64 切片（实际上是整数值）转换为字节切片
// 每个值先转换为 int64，然后以 uint64 格式存储（8 字节）
// 参数 ints: 输入的 float64 切片（应包含整数值）
// 返回值: 转换后的字节切片
func IntsToByteVector(ints []float64) []byte {
	vector := make([]byte, len(ints)*Uint64Len)
	for i, val := range ints {
		intVal := int64(val)
		binary.LittleEndian.PutUint64(vector[i*Uint64Len:(i+1)*Uint64Len], uint64(intVal))
	}
	return vector
}

// IntsFromByteVector 将字节切片转换回 int64 切片
// 参数 vector: 输入的字节切片
// 返回值: 转换后的 int64 切片
func IntsFromByteVector(vector []byte) []int64 {
	ints := make([]int64, len(vector)/Uint64Len)
	for i := 0; i < len(ints); i++ {
		asUint := binary.LittleEndian.Uint64(vector[i*Uint64Len : (i+1)*Uint64Len])
		ints[i] = int64(asUint)
	}
	return ints
}
