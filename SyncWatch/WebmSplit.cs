using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;

namespace SyncWatch;

sealed class WebmSplit
{
    internal const uint IdCluster = 0x1F43B675;
    internal const uint IdTimecode = 0xE7;
    internal const uint IdSimpleBlock = 0xA3;
    internal const uint IdBlock = 0xA1;
    internal const uint IdBlockGroup = 0xA0;
    internal const uint IdSegment = 0x18538067;
    internal const uint IdInfo = 0x1549A966;
    internal const uint IdTimestampScale = 0x2AD7B1;
    internal const int DefaultTimestampScale = 1_000_000;
    internal const int MaxBufferedBytes = 8 * 1024 * 1024;
    internal const int ClusterSplitRel = 15000;
    internal const int MaxClusterPayload = 0x0FFFFFFF;
    internal const int WalkGuard = 100000;
    internal const int HeaderGuard = 256;
    internal const int TimecodeGuard = 16;
    internal const int ShiftGuard = 64;
    internal const int ClusterIdBytes = 4;

    public readonly record struct Piece(byte[] Data, int TimestampMs);
    private readonly record struct Block(byte[] Element, int Rel, int TimeAt);

    private readonly MemoryStream _buf = new MemoryStream();
    private readonly List<Block> _blocks = new List<Block>();
    private bool _sawInit;
    private bool _inCluster;
    private bool _haveBase;
    private int _clusterBase;
    private int _clusterEnd = int.MaxValue;
    private int _scale = DefaultTimestampScale;

    public void Reset()
    {
        _buf.SetLength(0);
        _blocks.Clear();
        _sawInit = false;
        _inCluster = false;
        _haveBase = false;
        _clusterBase = 0;
        _clusterEnd = int.MaxValue;
        _scale = DefaultTimestampScale;
    }

    internal static bool StartsWithCluster(ReadOnlySpan<byte> data)
        => data.Length >= ClusterIdBytes && BinaryPrimitives.ReadUInt32BigEndian(data) == IdCluster;

    public static bool TrySplitInit(ReadOnlySpan<byte> data, out int clusterAt)
    {
        clusterAt = FindCluster(data, 0);
        return clusterAt > 0;
    }

    public static bool TryReadClusterTime(ReadOnlySpan<byte> data, out int units)
    {
        units = 0;
        if (data.Length < ClusterIdBytes + 2 || !StartsWithCluster(data))
            return false;
        if (!TryHeader(data, 0, out _, out uint id, out int sizeLen, out ulong size, out int header) || id != IdCluster)
            return false;
        int end = ClusterEnd(data.Length, header, sizeLen, size);
        return TryLocateTimecode(data, header, end, out _, out _, out units);
    }

    private const uint IdEbml = 0x1A45DFA3;
    private const uint IdTracks = 0x1654AE6B;

    internal static int InitStart(ReadOnlySpan<byte> data, int clusterAt)
    {
        int last = 0;
        int pos = 0;
        for (int guard = 0; guard < HeaderGuard && pos < clusterAt && pos < data.Length; guard++)
        {
            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header))
                break;
            if (id == IdEbml) last = pos;
            if (id == IdCluster) break;
            int value = pos + header;
            if (id == IdSegment || IsUnknown(size, sizeLen))
            {
                pos = value;
                continue;
            }

            if (size > int.MaxValue || (long)value + (long)size > data.Length || (long)value + (long)size <= pos)
                break;
            pos = value + (int)size;
        }
        return last;
    }

    internal static void InitSlice(ReadOnlySpan<byte> data, int clusterAt, out int start, out int end)
    {
        start = 0;
        end = clusterAt;
        int tracksEnd = -1;
        int pos = 0;
        for (int guard = 0; guard < HeaderGuard && pos < clusterAt && pos < data.Length; guard++)
        {
            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header))
                break;
            if (id == IdEbml) start = pos;
            int value = pos + header;
            bool unknown = IsUnknown(size, sizeLen);
            bool fits = !unknown && size <= int.MaxValue && (long)value + (long)size <= data.Length;
            if (id == IdTracks && fits)
                tracksEnd = value + (int)size;
            if (id == IdCluster) break;
            if (id == IdSegment || unknown)
            {
                pos = value;
                continue;
            }

            if (!fits || (long)value + (long)size <= pos) break;
            pos = value + (int)size;
        }

        if (tracksEnd > start && tracksEnd < clusterAt)
            end = tracksEnd;

        if (end - start > 600)
        {
            int lastMagic = start;
            int limit = Math.Min(end, data.Length) - 4;
            for (int i = start; i <= limit; i++)
            {
                if (BinaryPrimitives.ReadUInt32BigEndian(data.Slice(i, 4)) == IdEbml)
                    lastMagic = i;
            }
            if (lastMagic > start && end - lastMagic >= 32)
                start = lastMagic;
        }
    }

    public static int NoteTimes(ReadOnlySpan<byte> data, ref int clusterBase)
    {
        int max = clusterBase;
        int pos = 0;
        for (int guard = 0; guard < WalkGuard && pos + 2 <= data.Length; guard++)
        {
            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header))
                break;
            int value = pos + header;
            bool unknown = IsUnknown(size, sizeLen);
            bool fits = !unknown && size <= int.MaxValue && (long)value + (long)size <= data.Length;
            if (id == IdCluster)
            {
                int end = ClusterEnd(data.Length, value, sizeLen, size);
                if (TryLocateTimecode(data, value, end, out _, out _, out int units))
                {
                    clusterBase = units;
                    if (units > max) max = units;
                }
                pos = value;
                continue;
            }

            if (!fits || (long)value + (long)size <= pos) break;
            if ((id == IdSimpleBlock || id == IdBlock || id == IdBlockGroup) && clusterBase >= 0
                && TryBlockRel(data, id, value, value + (int)size, out int rel, out _))
            {
                int media = clusterBase + rel;
                if (media > max) max = media;
            }

            pos = value + (int)size;
        }

        return max;
    }

    public static byte[]? TryShiftNonNegative(byte[] data, int baseUnits)
    {
        if (baseUnits <= 0 || !StartsWithCluster(data)) return null;

        // Предварительная валидация без аллокации
        int pos = 0;
        bool needsChange = false;
        for (int guard = 0; guard < ShiftGuard && pos + ClusterIdBytes <= data.Length; guard++)
        {
            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header)) break;
            if (id != IdCluster) break;
            int end = ClusterEnd(data.Length, pos + header, sizeLen, size);
            if (TryLocateTimecode(data, pos + header, end, out _, out _, out int units))
            {
                int shifted = units - baseUnits;
                if (shifted < 0) return null;
                if (shifted != units) needsChange = true;
            }
            if (IsUnknown(size, sizeLen)) break;
            pos = end;
        }

        if (!needsChange) return data;

        byte[] copy = (byte[])data.Clone();
        pos = 0;
        for (int guard = 0; guard < ShiftGuard && pos + ClusterIdBytes <= copy.Length; guard++)
        {
            if (!TryHeader(copy, pos, out _, out uint id, out int sizeLen, out ulong size, out int header)) break;
            if (id != IdCluster) break;
            int end = ClusterEnd(copy.Length, pos + header, sizeLen, size);
            if (TryLocateTimecode(copy, pos + header, end, out int at, out int len, out int units))
            {
                int shifted = units - baseUnits;
                if (shifted != units)
                    WriteBe(copy.AsSpan(at, len), shifted);
            }
            if (IsUnknown(size, sizeLen)) break;
            pos = end;
        }

        return copy;
    }

    public static byte[] ShiftToZero(byte[] data, int baseUnits)
        => TryShiftNonNegative(data, baseUnits) ?? data;

    private static int ClusterEnd(int dataLength, int value, int sizeLen, ulong size)
    {
        if (IsUnknown(size, sizeLen) || size > int.MaxValue || (long)value + (long)size > dataLength)
            return dataLength;
        return value + (int)size;
    }

    private static bool TryLocateTimecode(ReadOnlySpan<byte> data, int from, int end, out int at, out int len, out int units)
    {
        at = 0;
        len = 0;
        units = 0;
        int pos = from;
        for (int guard = 0; guard < TimecodeGuard && pos + 2 <= end && pos < data.Length; guard++)
        {
            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header))
                return false;
            int value = pos + header;
            if (id == IdTimecode && size is >= 1 and <= 4 && (long)value + (long)size <= data.Length)
            {
                at = value;
                len = (int)size;
                units = ReadUint(data.Slice(value, len));
                return true;
            }

            if (id is IdSimpleBlock or IdBlock or IdBlockGroup) return false;
            if (IsUnknown(size, sizeLen) || size > int.MaxValue || (long)value + (long)size > end) return false;
            pos = value + (int)size;
        }

        return false;
    }

    private static void WriteBe(Span<byte> target, int value)
    {
        for (int i = target.Length - 1; i >= 0; i--)
        {
            target[i] = (byte)value;
            value >>= 8;
        }
    }

    public void Push(byte[] data, List<Piece> sink)
    {
        if (data.Length == 0) return;
        _buf.Write(data, 0, data.Length);
        if (_buf.Length > MaxBufferedBytes)
        {
            Reset();
            return;
        }

        byte[] all = _buf.ToArray();
        int pos = 0;
        Walk(all, ref pos, sink);
        _buf.SetLength(0);
        if (pos < all.Length)
            _buf.Write(all, pos, all.Length - pos);
    }

    private void Walk(byte[] data, ref int pos, List<Piece> sink)
    {
        for (int guard = 0; guard < WalkGuard && pos < data.Length; guard++)
        {
            if (_inCluster && pos >= _clusterEnd)
            {
                Flush(sink);
                _inCluster = false;
                _haveBase = false;
            }

            if (!_sawInit)
            {
                int at = FindCluster(data, pos);
                if (at <= pos) return;
                ReadScale(data.AsSpan(pos, at - pos));
                sink.Add(new Piece(data[pos..at], -1));
                _sawInit = true;
                pos = at;
                continue;
            }

            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header))
            {
                Flush(sink);
                return;
            }

            int value = pos + header;
            bool unknown = IsUnknown(size, sizeLen);
            bool fits = !unknown && size <= int.MaxValue && (long)value + (long)size <= data.Length;

            if (!_inCluster)
            {
                if (id == IdCluster)
                {
                    _inCluster = true;
                    _haveBase = false;
                    _clusterEnd = unknown || size > int.MaxValue ? int.MaxValue : value + (int)size;
                    pos = value;
                    continue;
                }

                if (!fits) return;
                pos = value + (int)size;
                continue;
            }

            if (id == IdCluster)
            {
                Flush(sink);
                _inCluster = false;
                _haveBase = false;
                continue;
            }

            if (!fits)
            {
                Flush(sink);
                return;
            }

            int end = value + (int)size;
            if (id == IdTimecode)
            {
                _clusterBase = ReadUint(data.AsSpan(value, (int)size));
                _haveBase = true;
            }
            else if (id is IdSimpleBlock or IdBlock or IdBlockGroup && TryBlockRel(data, id, value, end, out int rel, out int timeAt))
                _blocks.Add(new Block(data[pos..end], rel, timeAt - pos));

            pos = end;
        }

        Flush(sink);
    }

    private void Flush(List<Piece> sink)
    {
        if (!_haveBase) return;
        while (_blocks.Count > 0)
        {
            int min = int.MaxValue;
            foreach (Block block in _blocks)
                if (block.Rel < min) min = block.Rel;

            int take = _blocks.Count;
            for (int i = 0; i < _blocks.Count; i++)
            {
                if (_blocks[i].Rel - min > ClusterSplitRel)
                {
                    take = Math.Max(1, i);
                    break;
                }
            }

            List<Block> slice = _blocks.GetRange(0, take);
            _blocks.RemoveRange(0, take);
            Emit(sink, slice, min);
        }
    }

    private void Emit(List<Piece> sink, List<Block> slice, int minRel)
    {
        int units = _clusterBase + minRel;
        if (units < 0) units = 0;
        int ms = _scale == DefaultTimestampScale ? units : (int)((long)Math.Max(0, units) * _scale / DefaultTimestampScale);

        byte[] timecode = new byte[6];
        timecode[0] = (byte)IdTimecode;
        timecode[1] = 0x84;
        BinaryPrimitives.WriteInt32BigEndian(timecode.AsSpan(2, 4), units);

        int payload = timecode.Length;
        var blocks = new byte[slice.Count][];
        for (int i = 0; i < slice.Count; i++)
        {
            byte[] copy = (byte[])slice[i].Element.Clone();
            short rel = (short)(slice[i].Rel - minRel);
            int at = slice[i].TimeAt;
            BinaryPrimitives.WriteInt16BigEndian(copy.AsSpan(at, 2), rel);
            blocks[i] = copy;
            payload += copy.Length;
        }

        if (payload <= 0 || payload > MaxClusterPayload) return;

        byte[] cluster = new byte[8 + payload];
        BinaryPrimitives.WriteUInt32BigEndian(cluster.AsSpan(0, 4), IdCluster);
        cluster[4] = (byte)(0x10 | ((payload >> 24) & 0x0F));
        cluster[5] = (byte)(payload >> 16);
        cluster[6] = (byte)(payload >> 8);
        cluster[7] = (byte)payload;
        timecode.CopyTo(cluster, 8);
        int cursor = 14;
        foreach (byte[] block in blocks)
        {
            block.CopyTo(cluster, cursor);
            cursor += block.Length;
        }

        sink.Add(new Piece(cluster, ms));
    }

    public static void SelfCheck()
    {
        byte[] header =
        {
            0x1A, 0x45, 0xDF, 0xA3, 0x81, 0x00,
            0x18, 0x53, 0x80, 0x67, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0x15, 0x49, 0xA9, 0x66, 0x81, 0x00
        };
        byte[] open = { 0x1F, 0x43, 0xB6, 0x75, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

        var split = new WebmSplit();
        var sink = new List<Piece>();
        split.Push(header, sink);
        if (sink.Count != 0) throw new InvalidOperationException("header waits for a cluster");

        split.Push(Concat(open, new byte[] { 0xE7, 0x82, 0x03, 0xE8 }, Simple(0), Simple(200)), sink);
        if (sink.Count != 2) throw new InvalidOperationException("init and first cluster, got " + sink.Count);
        if (sink[0].TimestampMs != -1 || sink[0].Data.Length != header.Length)
            throw new InvalidOperationException("init");
        AssertCluster(sink[1], 1000, 0, 200);

        sink.Clear();
        split.Push(Simple(400), sink);
        if (sink.Count != 1) throw new InvalidOperationException("continuation");
        AssertCluster(sink[0], 1400, 0);

        var broken = new WebmSplit();
        var bad = new List<Piece>();
        broken.Push(Concat(header, open, Simple(10), new byte[] { 0xE7, 0x81, 50 }), bad);
        if (bad.Count != 2) throw new InvalidOperationException("reordered cluster");
        AssertCluster(bad[1], 60, 0);

        var magic = new WebmSplit();
        var hidden = new List<Piece>();
        magic.Push(Concat(header, open, new byte[] { 0xE7, 0x81, 1 }, SimpleMagic()), hidden);
        if (hidden.Count != 2) throw new InvalidOperationException("magic split");
        AssertCluster(hidden[1], 1, 0);

        byte[] cluster = Concat(open, new byte[] { 0xE7, 0x82, 0x03, 0xE8 }, Simple(200));
        if (!TryReadClusterTime(cluster, out int read) || read != 1000)
            throw new InvalidOperationException("cluster time");
        if (!ReferenceEquals(ShiftToZero(cluster, 0), cluster))
            throw new InvalidOperationException("no shift");
        byte[] shifted = ShiftToZero(cluster, 1000);
        if (!TryReadClusterTime(shifted, out int zero) || zero != 0)
            throw new InvalidOperationException("shifted time");
        if (shifted[^4] != 0x00 || shifted[^3] != 200)
            throw new InvalidOperationException("block rel changed");

        int clusterBase = -1;
        int media = NoteTimes(cluster, ref clusterBase);
        if (clusterBase != 1000 || media != 1200)
            throw new InvalidOperationException("media end " + media);

        byte[] doubled = Concat(header, header, cluster);
        int splitAt = FindCluster(doubled, 0);
        if (splitAt <= header.Length || InitStart(doubled, splitAt) != header.Length)
            throw new InvalidOperationException("init trimmed to the last header");

        byte[] tracks = { 0x16, 0x54, 0xAE, 0x6B, 0x81, 0x00 };
        byte[] junk = { 0xEC, 0x81, 0x00 };
        byte[] packed = Concat(header, tracks, junk, cluster);
        int packedAt = FindCluster(packed, 0);
        InitSlice(packed, packedAt, out int sliceStart, out int sliceEnd);
        if (sliceStart != 0 || sliceEnd != header.Length + tracks.Length)
            throw new InvalidOperationException("init ends at tracks " + sliceEnd);
    }

    private static void AssertCluster(Piece piece, int timecode, params int[] rels)
    {
        byte[] data = piece.Data;
        if (piece.TimestampMs != timecode) throw new InvalidOperationException("timestamp " + piece.TimestampMs);
        if (!TryHeader(data, 0, out _, out uint id, out _, out ulong size, out int header) || id != IdCluster)
            throw new InvalidOperationException("cluster id");
        int pos = header;
        int end = pos + (int)size;
        if (!TryHeader(data, pos, out _, out uint first, out _, out ulong tcSize, out int tcHeader) || first != IdTimecode)
            throw new InvalidOperationException("timecode must lead");
        int value = ReadUint(data.AsSpan(pos + tcHeader, (int)tcSize));
        if (value != timecode) throw new InvalidOperationException("timecode value " + value);
        pos += tcHeader + (int)tcSize;
        foreach (int rel in rels)
        {
            if (!TryHeader(data, pos, out _, out uint blockId, out _, out ulong blockSize, out int blockHeader) || blockId != IdSimpleBlock)
                throw new InvalidOperationException("block");
            int blockValue = pos + blockHeader;
            if (!TryReadVint(data, blockValue, out int trackLen, out _))
                throw new InvalidOperationException("track");
            int got = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(blockValue + trackLen, 2));
            if (got != rel) throw new InvalidOperationException("rel " + got);
            pos = blockValue + (int)blockSize;
        }

        if (pos != end) throw new InvalidOperationException("cluster tail");
    }

    private static byte[] Simple(int rel) => new byte[] { 0xA3, 0x85, 0x81, (byte)(rel >> 8), (byte)rel, 0x80, 0xAA };

    private static byte[] SimpleMagic() => new byte[] { 0xA3, 0x88, 0x81, 0x00, 0x00, 0x80, 0x1F, 0x43, 0xB6, 0x75 };

    private static byte[] Concat(params byte[][] parts)
    {
        int n = 0;
        foreach (byte[] part in parts) n += part.Length;
        byte[] all = new byte[n];
        int p = 0;
        foreach (byte[] part in parts)
        {
            part.CopyTo(all, p);
            p += part.Length;
        }
        return all;
    }

    private void ReadScale(ReadOnlySpan<byte> init)
    {
        int pos = 0;
        for (int guard = 0; guard < HeaderGuard && pos < init.Length; guard++)
        {
            if (!TryHeader(init, pos, out _, out uint id, out int sizeLen, out ulong size, out int header)) return;
            int value = pos + header;
            if (id == IdTimestampScale && size is >= 1 and <= 8 && (long)value + (long)size <= init.Length)
            {
                int scale = ReadUint(init.Slice(value, (int)size));
                if (scale > 0) _scale = scale;
                return;
            }

            if (id is IdSegment or IdInfo)
            {
                pos = value;
                continue;
            }

            if (IsUnknown(size, sizeLen) || size > int.MaxValue || (long)value + (long)size > init.Length) return;
            pos = value + (int)size;
        }
    }

    private static int FindCluster(ReadOnlySpan<byte> data, int from)
    {
        int pos = from;
        for (int guard = 0; guard < HeaderGuard && pos < data.Length; guard++)
        {
            if (!TryHeader(data, pos, out _, out uint id, out int sizeLen, out ulong size, out int header))
                return -1;
            int value = pos + header;
            if (id == IdCluster) return pos;
            if (id == IdSegment)
            {
                pos = value;
                continue;
            }

            if (IsUnknown(size, sizeLen) || size > int.MaxValue || (long)value + (long)size > data.Length)
                return -1;
            pos = value + (int)size;
        }

        return -1;
    }

    private static bool TryBlockRel(ReadOnlySpan<byte> data, uint id, int value, int end, out int rel, out int timeAt)
    {
        rel = 0;
        timeAt = 0;
        int payload = value;
        if (id == IdBlockGroup)
        {
            int cursor = value;
            bool found = false;
            while (cursor + 2 <= end)
            {
                if (!TryHeader(data, cursor, out _, out uint child, out int childSizeLen, out ulong childSize, out int childHeader))
                    return false;
                int childValue = cursor + childHeader;
                if (IsUnknown(childSize, childSizeLen) || (long)childValue + (long)childSize > end) return false;
                if (child == IdBlock)
                {
                    payload = childValue;
                    found = true;
                    break;
                }

                cursor = childValue + (int)childSize;
            }

            if (!found) return false;
        }

        if (!TryReadVint(data, payload, out int trackLen, out _)) return false;
        timeAt = payload + trackLen;
        if (timeAt + 2 > end) return false;
        rel = BinaryPrimitives.ReadInt16BigEndian(data.Slice(timeAt, 2));
        return true;
    }

    private static bool TryHeader(ReadOnlySpan<byte> data, int pos, out int idLen, out uint id, out int sizeLen, out ulong size, out int header)
    {
        idLen = 0;
        id = 0;
        sizeLen = 0;
        size = 0;
        header = 0;
        if (!TryReadId(data, pos, out idLen, out id)) return false;
        if (!TryReadVint(data, pos + idLen, out sizeLen, out size)) return false;
        header = idLen + sizeLen;
        return pos <= data.Length - header;
    }

    private static int ReadUint(ReadOnlySpan<byte> data)
    {
        long value = 0;
        for (int i = 0; i < data.Length; i++) value = (value << 8) | data[i];
        return value > int.MaxValue ? int.MaxValue : (int)value;
    }

    private static bool TryReadId(ReadOnlySpan<byte> data, int pos, out int len, out uint id)
    {
        len = 0;
        id = 0;
        if ((uint)pos >= (uint)data.Length) return false;
        byte first = data[pos];
        if (first == 0) return false;
        len = 1;
        int mask = 0x80;
        while ((first & mask) == 0)
        {
            len++;
            mask >>= 1;
            if (len > 4) return false;
        }

        if (pos + len > data.Length) return false;
        for (int i = 0; i < len; i++) id = (id << 8) | data[pos + i];
        return true;
    }

    private static bool TryReadVint(ReadOnlySpan<byte> data, int pos, out int len, out ulong value)
    {
        len = 0;
        value = 0;
        if ((uint)pos >= (uint)data.Length) return false;
        byte first = data[pos];
        if (first == 0) return false;
        len = 1;
        int mask = 0x80;
        while ((first & mask) == 0)
        {
            len++;
            mask >>= 1;
            if (len > 8) return false;
        }

        if (pos + len > data.Length) return false;
        value = (ulong)(first & (mask - 1));
        for (int i = 1; i < len; i++) value = (value << 8) | data[pos + i];
        return true;
    }

    private static bool IsUnknown(ulong value, int width)
    {
        if (width is < 1 or > 8) return false;
        ulong mask = (1UL << (7 * width)) - 1;
        return value == mask;
    }
}