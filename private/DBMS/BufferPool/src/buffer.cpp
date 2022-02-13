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
  numBufs = bufs; // Don't we need to do this?
  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  clockHand = ++clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId& fr) {
  File file = bufDescTable[clockHand].file;
  PageId pageNo = bufDescTable[clockHand].pageNo;

  int count = 1;
  while(count < numBufs) {
    BufDesc frame = BufMgr::bufDescTable[clockHand];
    if (frame.valid == true){
      if(frame.refbit == true) {
        //clear refbit 
        frame.refbit = false;
        BufMgr::advanceClock();
      }else {
        if(frame.pinCnt > 0){
          BufMgr::advanceClock();
        } else {
          if(frame.dirty == true) {
            //flush page to disk
            try {
              flushFile(bufDescTable[clockHand].file);
            }catch (BadBufferException e){
              
            }
            //call Set() on frame
            frame.Set(file, pageNo);
            fr = clockHand;
          }
          else
          {
            //use current frame, call Set()
            frame.Set(file, pageNo);
            fr = clockHand;
          }
        }
      }
    } else {
      //call Set() on the frame
      frame.Set(file, pageNo);
      fr = clockHand;
    }
    count++;
  }
  //while loop ended, so all frames scanned and found no pinCnt = 0
  throw BufferExceededException();

}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId frameNo = 0;
  
  // check if page is in the hashtable
  try {
    hashTable.lookup(file, pageNo, frameNo);
    //page is bufferPool so set refBit, increase pinCnt for page
    for (BufDesc desc : bufDescTable)
    {
      if(desc.pageNo == pageNo){
        frameNo = desc.frameNo;
        desc.refbit = true;
        desc.pinCnt++;
        break;
      }
    }
    for (uint32_t i = 0; i < numBufs; i++)
    {
      if(bufPool[i] == *page) {
        //return a pointer to the frame containing page
        page = &desc.frameNo;
      }
    }
  }
  catch (HashNotFoundException e)
  {
    //page not in buffer pool so call allocBuf()
    allocBuf(frameNo);
    //read in page from disk
    file.readPage(pageNo);
    //insert page into hashtable
    hashTable.insert(file, pageNo, frameNo);
  }
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {
//Scan bufTable for pages belonging to the file
  for(BufDesc frame : bufDescTable) {
    //if page is dirty call file.writepage() then set dirty bit to false
    if (frame.file == file) {
      if (frame.valid == false) {
        throw BadBufferException(frame.frameNo, frame.dirty, frame.valid, frame.refbit);
      }
      if(frame.dirty){
      //file.writePage(frame.pageNo, Page)
        file.writePage(frame.pageNo, bufPool[frame.frameNo]);
        frame.dirty = false;
      }
      //if (file is pinned)
      if (frame.pinCnt > 0) {
        throw PagePinnedException(frame.file.filename_, frame.pageNo, frame.frameNo);
      }
      //remove, remove(file, pagenumber), the page from hashtable
      hashTable.remove(file, frame.pageNo);
      //clear() method of BufDesc for the page frame 
      frame.clear();
      frame.pageNo = -1;
      frame.valid = false;
    }
  }
}

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
