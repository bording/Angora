using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Angora
{
    ref struct CustomBufferReader
    {
        ReadOnlySpan<byte> _currentSpan;
        int _index;

        ReadOnlySequence<byte> _sequence;
        SequencePosition _currentSequencePosition;
        SequencePosition _nextSequencePosition;

        int _consumedBytes;
        bool _end;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CustomBufferReader(in ReadOnlySequence<byte> buffer)
        {
            _index = 0;
            _consumedBytes = 0;
            _sequence = buffer;
            _currentSequencePosition = _sequence.Start;
            _nextSequencePosition = _currentSequencePosition;

            if (_sequence.TryGet(ref _nextSequencePosition, out var memory, true))
            {
                _end = false;
                _currentSpan = memory.Span;
                if (_currentSpan.Length == 0)
                {
                    // No space in first span, move to one with space
                    MoveNext();
                }
            }
            else
            {
                // No space in any spans and at end of sequence
                _end = true;
                _currentSpan = default;
            }
        }

        public bool End => _end;

        public int CurrentSegmentIndex => _index;

        public SequencePosition Position => _sequence.GetPosition(_index, _currentSequencePosition);

        public ReadOnlySpan<byte> CurrentSegment => _currentSpan;

        public ReadOnlySpan<byte> UnreadSegment => _currentSpan.Slice(_index);

        public int ConsumedBytes => _consumedBytes;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Peek()
        {
            if (_end)
            {
                return -1;
            }
            return _currentSpan[_index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read()
        {
            if (_end)
            {
                return -1;
            }

            var value = _currentSpan[_index];
            _index++;
            _consumedBytes++;

            if (_index >= _currentSpan.Length)
            {
                MoveNext();
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            var value = _currentSpan[_index];
            _index++;
            _consumedBytes++;

            if (_index >= _currentSpan.Length)
            {
                MoveNext();
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sbyte ReadSByte()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            var value = _currentSpan[_index];
            _index++;
            _consumedBytes++;

            if (_index >= _currentSpan.Length)
            {
                MoveNext();
            }

            return (sbyte)value; //TODO check this cast
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadInt16()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            if (BinaryPrimitives.TryReadInt16BigEndian(_currentSpan, out var value))
            {
                _index += sizeof(short);
                _consumedBytes += sizeof(short);

                if (_index >= _currentSpan.Length)
                {
                    MoveNext();
                }
            }
            else
            {
                value = 0; //TODO figure out what to actually do here
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ReadUInt16()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            if (BinaryPrimitives.TryReadUInt16BigEndian(_currentSpan, out var value))
            {
                _index += sizeof(ushort);
                _consumedBytes += sizeof(ushort);

                if (_index >= _currentSpan.Length)
                {
                    MoveNext();
                }
            }
            else
            {
                value = 0; //TODO figure out what to actually do here
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt32()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            if (BinaryPrimitives.TryReadInt32BigEndian(_currentSpan, out var value))
            {
                _index += sizeof(int);
                _consumedBytes += sizeof(int);

                if (_index >= _currentSpan.Length)
                {
                    MoveNext();
                }
            }
            else
            {
                value = 0; //TODO figure out what to actually do here
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ReadUInt32()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            if (BinaryPrimitives.TryReadUInt32BigEndian(_currentSpan, out var value))
            {
                _index += sizeof(uint);
                _consumedBytes += sizeof(uint);

                if (_index >= _currentSpan.Length)
                {
                    MoveNext();
                }
            }
            else
            {
                value = 0; //TODO figure out what to actually do here
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadInt64()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            if (BinaryPrimitives.TryReadInt64BigEndian(_currentSpan, out var value))
            {
                _index += sizeof(long);
                _consumedBytes += sizeof(long);

                if (_index >= _currentSpan.Length)
                {
                    MoveNext();
                }
            }
            else
            {
                value = 0; //TODO figure out what to actually do here
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadUInt64()
        {
            if (_end)
            {
                return 0; // TODO change this to throw instead?
            }

            if (BinaryPrimitives.TryReadUInt64BigEndian(_currentSpan, out var value))
            {
                _index += sizeof(ulong);
                _consumedBytes += sizeof(ulong);

                if (_index >= _currentSpan.Length)
                {
                    MoveNext();
                }
            }
            else
            {
                value = 0; //TODO figure out what to actually do here
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ReadFloat()
        {
            var value = ReadUInt32();
            var span = MemoryMarshal.CreateReadOnlySpan(ref value, 1);
            var bytesSpan = MemoryMarshal.AsBytes(span);

            return MemoryMarshal.Read<float>(bytesSpan);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double ReadDouble()
        {
            var value = ReadUInt64();
            var span = MemoryMarshal.CreateReadOnlySpan(ref value, 1);
            var bytesSpan = MemoryMarshal.AsBytes(span);

            return MemoryMarshal.Read<double>(bytesSpan);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void MoveNext()
        {
            var previous = _nextSequencePosition;
            while (_sequence.TryGet(ref _nextSequencePosition, out var memory, true))
            {
                _currentSequencePosition = previous;
                _currentSpan = memory.Span;
                _index = 0;
                if (_currentSpan.Length > 0)
                {
                    return;
                }
            }
            _end = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(int byteCount)
        {
            if (!_end && byteCount > 0 && (_index + byteCount) < _currentSpan.Length)
            {
                _consumedBytes += byteCount;
                _index += byteCount;
            }
            else
            {
                AdvanceNext(byteCount);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void AdvanceNext(int byteCount)
        {
            if (byteCount < 0)
            {
                BuffersThrowHelper.ThrowArgumentOutOfRangeException(BuffersThrowHelper.ExceptionArgument.length);
            }

            _consumedBytes += byteCount;

            while (!_end && byteCount > 0)
            {
                if ((_index + byteCount) < _currentSpan.Length)
                {
                    _index += byteCount;
                    byteCount = 0;
                    break;
                }

                var remaining = (_currentSpan.Length - _index);

                _index += remaining;
                byteCount -= remaining;

                MoveNext();
            }

            if (byteCount > 0)
            {
                BuffersThrowHelper.ThrowArgumentOutOfRangeException(BuffersThrowHelper.ExceptionArgument.length);
            }
        }
    }
}
