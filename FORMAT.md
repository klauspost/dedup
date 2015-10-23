# Stream formats

# Format 1
This format has data and index split in two files, so the index can be quickly read before any decoding starts.

This index allows to keep track of the last occurence of a block, so it can be deallocated immediately afterwards.
 
## Header

| Content        | Type    | Values       |
|----------------|---------|--------------|
| Format ID      | UvarInt | 0x1 (always) |
| MaxBlockSize | UvarInt |  > 512       |

## Repeat Blocks

This is the index parsing algorithm in overall terms.

```Go
for {
    offset = ReadVarUint()
    
    switch offset {
    
    // NEW BLOCK 
     case 0:
        x = ReadVarUint()
        if x > MaxBlockSize { ERROR }
        blockSize = MaxBlockSize - x        
        block = ReadBytesFromDataStream(blockSize)   
 
    // END OF STREAM
    case  1<<64 - 1:
        x = ReadVarUint()
        if x > MaxBlockSize { ERROR }
        blockSize = MaxBlockSize - x
        block = ReadBytesFromDataStream(blockSize)

        // Stream terminator
        x := ReadVarUint()
        if x != 0 { ERROR }        
        break
        
    // DEDUPLICATED BLOCK
    default:
		SourceBlockNum = CurrentBlock - offset
        if SourceBlockNum < 0 { ERROR }
    }    
}
```

### Block sizes
Block sizes are stored as `MaxSize - Size`, so fixed block sizes are all stored as size '0'.

### Block Offset
The deduplicated offset is backwards from the the current block, so if the current block is the same 
as the previous, it will be encoded as '1'. If it is two blocks back, 2, etc.
  
# Format 2

Format 2 has block definitions and data interleaved. It only has a minor difference to Format 1, since it includes a 
Maximum backreference Length, helping the decoder to deallocate blocks.
 

## Header

| Content        | Type    | Values       |
|----------------|---------|--------------|
| Format ID      | UvarInt | 0x2 (always) |
| MaxBlockSize | UvarInt |  > 512       |
| MaxLength | UvarInt |  > 1       |

In addition to Maximum Block Size, a `MaxLength` is also added, which indicates the maximum backreference distance 
of this stream. This means that any offsets will be less or equal to MaxLength.

## Repeat Blocks

This is the decoding loop. `MaxLength` blocks should be kept in memory while the decoding is taking place. 
Data is read from in between block definitions, if offset is 0.

```Go
for {
    offset = ReadVarUint()
    
    switch offset {
    
    // NEW BLOCK 
     case 0:
        x = ReadVarUint()
        if x > MaxBlockSize { ERROR }
        blockSize = MaxBlockSize - x        
        block = ReadBytes(blockSize)   
 
    // END OF STREAM
    case  1<<64 - 1:
        x = ReadVarUint()
        if x > MaxBlockSize { ERROR }
        blockSize = MaxBlockSize - x
        block = ReadBytes(blockSize)

        // Stream terminator
        x := ReadVarUint()
        if x != 0 { ERROR }        
        break
        
    // DEDUPLICATED BLOCK
    default:
        if offset > MaxLength { ERROR }
		SourceBlockNum = CurrentBlock - offset
        if SourceBlockNum < 0 { ERROR }
    }    
}

```


