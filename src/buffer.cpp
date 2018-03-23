/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Group 38
 * Team members:
 * 1. Benjamin Klonowski  - ??
 * 2. Kyle Nelsen         - ??
 * 3. Sapan Gupta         - 9077980283
 *
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  /// Flush out all dirty pages to disk, remove all page entries from hashTable
  /// (do not need to clear bufPool entry, if no page entry in hashTable)
  /// and deallocate buffer pool and bufDesc table array object.
  for (FrameId i = 0; i < this->numBufs; i++)
  { 
    if (this->bufDescTable[i].dirty)
    {
      this->bufDescTable[i].file->writePage(this->bufPool[i]);
      this->bufDescTable[i].dirty = false;
    }    
    this->hashTable->remove(this->bufDescTable[i].file, this->bufDescTable[i].pageNo);
  }
  delete[] bufDescTable;
  delete[] bufPool; 
  bufDescTable = NULL;
  bufPool = NULL;
}

void BufMgr::advanceClock()
{
  /**
   * Increment clockHand if it is less than the highest buffer page index.
   * If equal to the highest buffer frame index, set clockHand to 0.
   */
  if (this->clockHand < numBufs - 1) 
	  this->clockHand++;
  else if (this->clockHand == numBufs - 1)
	  this->clockHand = 0;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    int flag = 0;
    std::uint32_t  pinned = 0;
    /**
     * Loop through the frames till a unpinned frame is found 
     * or no unpinned frame exists (throw BufferExceededException)
     */
    while(pinned < this->numBufs-1)
    {
      BufMgr::advanceClock();  //Advance the clock
      BufDesc desc =  *(bufDescTable + frame);
      /// if frame is valid and
      ///   if refbit set, unset refbit and continue
      ///   else if frame pinned, continue
      ///   else if frame dirty, flush the page and unset the dirty flag
      /// else
      if(desc.valid) 
      {
        if (desc.refbit)
        {
          desc.refbit = false;
          continue;
        }
        else if (desc.pinCnt > 0)
        {
          pinned++;
          continue;
        }
        else if (desc.dirty) //Check if the dirty bit is set 
        {
            desc.file->writePage(bufPool[frame]); //If yes, write the page in desc
            desc.dirty = false;
        }
        // remove the page from the hashTable
        this->hashTable->remove(desc.file, desc.pageNo);
      }
      flag = 1;
      break;
    }
    if (!flag)
        throw BufferExceededException();
}
	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId frameNo = 69; 
    try 
    {
      /**
       * Case 2: Page is in the buffer pool.
     	 * Update the refbit and increment the pin count
    	 * return a pointer to the buffer frame that references the page
    	 */
    	hashTable->lookup(file, pageNo, frameNo); // Look for the page in the pool
     	bufDescTable[frameNo].refbit = true;
    	bufDescTable[frameNo].pinCnt++;
    	page = &bufPool[frameNo];	 
    } 
    catch (HashNotFoundException& e) 
    {
      /**
       * Case 1: Page is NOT in the buffer pool.
       * If the page is not in the buffer pool, call allocBuff() method
       * once a buffer frame is allocated, call file->readPage() to get the page.
       * Place the page in the buffer pool, insert the page into the hash table 
       * and call Set() to set the page. 
       * Reference argument page will return a pointer to the frame where the page is pinned
       */
    	this->allocBuf(this->clockHand); // allocate the buffer frame pointed to by clockHand for the page
    	*page = file->readPage(pageNo); // read the page in from memory
      this->bufPool[this->clockHand] = *page; // set the page in the buffer pool
    	this->hashTable->insert(file, pageNo,this->clockHand); // place the page into the buffer frame	
    	this->bufDescTable[this->clockHand].Set(file, pageNo); // call to set the BufDesc properly	
    }
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  /// if frame containing page (file, pageNo) is pinned, throw exception,
  /// else decrement pinCnt of the frame 
  /// and set the dirty flag is the provided argument, dirty, is true
  FrameId frameNo;
  try
  {
    this->hashTable->lookup(file, pageNo, frameNo);
    if (this->bufDescTable[frameNo].pinCnt == 0)
    {
      throw PageNotPinnedException(file->filename(), pageNo, frameNo);
    }
    this->bufDescTable[frameNo].pinCnt--;
    if (dirty == true)
      this->bufDescTable[frameNo].dirty = true;
  }
  catch (HashNotFoundException& e)
  {
    /**
     * do nothing
     */
  }
}

void BufMgr::flushFile(const File* file) 
{
  /**
   * scan bufDesc Table for pages belonging to file
   * and check for existence of pinned and invalid pages belonging to the file
   */
  for (FrameId i = 0; i < this->numBufs-1; i++)
  {
    if (bufDescTable[i].file == file)
    {
      /// for the found page of the given file,
      /// check if it is pinned or invalid and throw corresponding exception, if it is
      if (bufDescTable[i].pinCnt > 0)
        throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
      if (bufDescTable[i].valid == false)
        throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
    }
  }
  /**
   * scan bufDesc Table again for the pages belonging to the file
   * if page is dirty, flush to disk and unset dirty flag
   * remove the page entry from hashTable and clear frame description
   */
  for (FrameId i = 0; i < this->numBufs-1; i++)
  {
    if (bufDescTable[i].file == file)
    {
      /// if the page is not pinned or invalid,
      /// flush page to disk, if page is dirty
      if (bufDescTable[i].dirty)
      {
        bufDescTable[i].file->writePage(this->bufPool[i]);
        bufDescTable[i].dirty = false;
      }
      /// remove page entry from hashTable
      /// (do not need to clear bufPool entry, if no page entry in hashTable)
      /// and clear description for the page buf frame
      this->hashTable->remove(file, bufDescTable[i].pageNo);
      this->bufDescTable[i].Clear();
    }
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  /// allocate an empty page in file
  /// and obtain buffer pool frame
  *page = file->allocatePage();
  pageNo = (*page).page_number();
  FrameId frameNo;
  this->allocBuf(frameNo);
  // TODO: Should the RHS of assignment be page or *page?
  this->bufPool[frameNo] = *page;
  /// insert an entry into hashTable
  /// set the frame description
  this->hashTable->insert(file, pageNo, frameNo);
  this->bufDescTable[frameNo].Set(file, pageNo);
}

void BufMgr::disposePage(File* file, const PageId pageNo)
{
  FrameId frameNo;
  /// find page buf frame's frameNo corresponding to given file and pageNo.
  /// not handling HashNotFoundException as according to Minh Le 
  /// on piazza @342, caller can catch and handle it.
  this->hashTable->lookup(file, pageNo, frameNo);
  /// if frame is pinned
  ///   throw PagePinnedException (as per Xiuting Wang [piazza @347])
  /// else
  ///   clear description for page buf frame,
  ///   remove hashTable entry for file and pageNo
  ///   and delete page from bufPool
  if (this->bufDescTable[frameNo].pinCnt > 0)
    throw PagePinnedException(file->filename(), pageNo, frameNo);  
  this->bufDescTable[frameNo].Clear();
  this->hashTable->remove(file, pageNo);
  // TODO: do we need to set the page entry in bufPool to NULL explicitly?
  // this->bufPool[frameNo] = NULL;
  /** 
   * delete page from file
   */
  file->deletePage(pageNo);    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
