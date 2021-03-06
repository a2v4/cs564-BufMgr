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
    if (bufDescTable[clockHand].valid == true)
    {
      if (bufDescTable[clockHand].refbit == true)
      {
        bufDescTable[clockHand].refbit = false;
        continue; // advance clock and try again
      }
      else if (bufDescTable[clockHand].pinCnt > 0)
      {
        continue; // advance clock and try again
      }
      else if (bufDescTable[clockHand].refbit == false && bufDescTable[clockHand].pinCnt == 0)
      {
        // Use the frame
        frame = clockHand;

        if (bufDescTable[clockHand].dirty)
        {
          // Flush page to disk
          bufDescTable[clockHand].file.writePage(bufPool[frame]);
        }
        hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
      }
      openFrameFound = true;
      bufDescTable[clockHand].clear();
    }
    else
    {
      // Use the frame
      frame = clockHand;
      openFrameFound = true;
      bufDescTable[clockHand].clear();
    }
  }
  if (openFrameFound == false)
  {
    throw BufferExceededException();
  }
}

void BufMgr::readPage(File &file, const PageId pageNo, Page *&page)
{

  FrameId frameNo; // to be filled in by hashTable.lookup
  // check if page is in the hashtable
  try
  {
    // Check if page is in hashTable
    hashTable.lookup(file, pageNo, frameNo);
    // Case 2

    // set the appropriate refbit
    bufDescTable[frameNo].refbit = true;
    // increment the pinCnt for the page
    bufDescTable[frameNo].pinCnt++;
  }
  catch (HashNotFoundException& e)
  {
    // Case 1
    // Call allocBuf() to allocate a buffer frame
    allocBuf(frameNo);

    // Call the method file.readPage() to read the page
    // from disk into the buffer pool frame.
    Page newPage = file.readPage(pageNo);
    // Add newPage to bufPool at the index 'frameNo'
    bufPool[frameNo] = newPage;

    // Next, insert the page into the hashtable
    hashTable.insert(file, pageNo, frameNo);

    // Finally, invoke Set() on the frame to set it up properly
    bufDescTable[frameNo].Set(file, pageNo);
  }
    // Return a pointer to the frame containing 
    // the page via the page parameter.
    page = &bufPool[frameNo];
}

void BufMgr::unPinPage(File &file, const PageId pageNo, const bool dirty)
{
  try
  {
    // Check if page is in hashTable
    FrameId frameNum; // to be replaced by the hashTable.lookup
    hashTable.lookup(file, pageNo, frameNum);

    if (bufDescTable[frameNum].pinCnt == 0)
    {
      // Throws PAGENOTPINNED if the pin count is already 0
      throw PageNotPinnedException(file.filename_, pageNo, frameNum);
    }
    else if (bufDescTable[frameNum].pinCnt > 0)
    {
      // Decrements the pinCnt of the frame
      --bufDescTable[frameNum].pinCnt;
    }
    if (dirty)
    {
      // if dirty == true, sets the dirty bit
      bufDescTable[frameNum].dirty = true;
    }
  }
  catch (HashNotFoundException &e)
  {
    // Does nothing if page is not found in the hash table lookup.
    return;
  }
}

void BufMgr::allocPage(File &file, PageId &pageNo, Page *&page)
{
  // The first step in this method is to allocate an empty page
  // in the specified file by invoking the file.allocatePage() method
  // This method will return a newly allocated page.
  Page newPage = file.allocatePage();

  // Then allocBuf() is called to obtain a buffer pool frame.
  FrameId newFrameId;
  allocBuf(newFrameId);
  // add newPage to bufPool based on newFrameId index
  bufPool[newFrameId] = newPage;

  // The method returns both the page number of the
  // newly allocated page to the caller via the pageNo
  // parameter and a pointer to the buffer frame allocated
  // for the page via the page parameter.
  page = &bufPool[newFrameId];
  pageNo = newPage.page_number();

  // Next, an entry is inserted into the hash table and Set() is
  // invoked on the frame to set it up properly
  hashTable.insert(file, pageNo, newFrameId);
  bufDescTable[newFrameId].Set(file, pageNo);
}

void BufMgr::flushFile(File &file)
{
  // Scan bufTable for pages belonging to the file
  // for (BufDesc frame : bufDescTable)
  for (u_int32_t i = 0; i < numBufs; i++)
  {
    //if page is dirty call file.writepage() then set dirty bit to false
    if (bufDescTable[i].file == file)
    {
      if (bufDescTable[i].valid == false)
      {
        throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      // Throws PagePinnedException if some page of the file is pinned.
      if (bufDescTable[i].pinCnt > 0)
      {
        throw PagePinnedException(bufDescTable[i].file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
      }
      if (bufDescTable[i].dirty)
      {
        // if the page is dirty, call file.writePage() to flush the page to disk
        // and then set the dirty bit for the page to false

        // file.writePage(bufDescTable[i].pageNo, Page)
        // bufDescTable[i].file.writePage(bufDescTable[i].pageNo, bufDescTable[i].pageNo);
        bufDescTable[i].file.writePage(bufPool[i]);
        bufDescTable[i].dirty = false;
      }
      // Throws BadBufferException if an invalid page belonging to the file is encountered
      if (Page::INVALID_NUMBER == bufDescTable[i].pageNo)
      {
        throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      }
      // remove the page from the hashtable (whether the page is clean or dirty)
      hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo);

      // invoke the Clear() method of BufDesc for the page frame
      bufDescTable[i].clear();
    }
  }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  try {
    FrameId frameNo; // blank frameNo to use for search
    hashTable.lookup(file, PageNo, frameNo);
    hashTable.remove(file, PageNo);
    bufDescTable[frameNo].clear();
  } catch (HashNotFoundException& e) {
    // not found, move on...
  }
  
  // Delete page from file
  file.deletePage(PageNo);
}

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
