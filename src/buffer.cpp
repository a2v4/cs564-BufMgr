/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
  bool openFrameFound = false;
  int counter = -1;
  while (counter < (int)numBufs && openFrameFound == false)
  {
    counter++;
    advanceClock();
    BufDesc currFrame = bufDescTable[clockHand];
    if (currFrame.valid == true)
    {
      if (currFrame.refbit == true)
      {
        currFrame.refbit = false;
        continue; // advance clock and try again
      }
      else if (currFrame.pinCnt > 0)
      {
        continue; // advance clock and try again
      }
      else if (currFrame.refbit == false && currFrame.pinCnt == 0)
      {
        // Use the frame
        frame = clockHand;

        if (currFrame.dirty)
        {
          // Flush page to disk
          currFrame.file.writePage(bufPool[frame]);
        }
        hashTable.remove(currFrame.file, currFrame.pageNo);
      }
      openFrameFound = true;
      currFrame.clear();
    }
    else
    {
      // Use the frame
      frame = clockHand;
      openFrameFound = true;
      currFrame.clear();
    }
  }
  if (openFrameFound == false)
  {
    throw BufferExceededException();
  }
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
