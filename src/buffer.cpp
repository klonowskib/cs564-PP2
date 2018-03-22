/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
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
  //Flush out all dirty pages and deallocate individual items in buffer pool and bufDesc table
  //for (FrameId i = 0; i < this->numBufs; i++) {
  //  this->flushFile(bufDescTable[i].file);
  //  delete bufDescTable[i];
  //  delete bufPool[i];
  //}
  // Deallocate buffer pool and bufDesc table array object
  delete[] bufDescTable;
  delete[] bufPool; 
  bufDescTable = NULL;
  bufPool = NULL;
}

void BufMgr::advanceClock()
{	
    if (this->clockHand < numBufs - 1) 
	this->clockHand++; ///Increment clockHand if it is less than the highest buffer page index.
    else if (this->clockHand == numBufs - 1)
	this->clockHand = 0;	///If equal to the highest buffer frame index, set clockHand to 0.
}

void BufMgr::allocBuf(FrameId & frame) 
{
    int flag = 0;
    std::uint32_t  pinned = 0;
    while(pinned < this->numBufs-1)
    {
      BufMgr::advanceClock();  //Advance the clock
      BufDesc desc =  *(bufDescTable + frame);
      if(desc.valid) 
      { //Check the valid bit for the frame
        if (desc.refbit) //Check if the refbit is set
        {
          desc.refbit = false; //Clear refbit if set  
          continue;
        }
        else if (desc.pinCnt > 0) //Check if page is pinned
        {
          pinned++;
          continue;
        }
        else if (desc.dirty) //Check if the dirty bit is set 
        {
            desc.file->writePage(bufPool[frame]); //If yes, write the page in desc
            desc.dirty = false;
        }
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
    	///Case 2: Page is in the buffer pool.
       	/**
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
      ///Case 1: Page is NOT in the buffer pool.
      /**
       * If the page is not in the buffer pool, call allocBuff() method
       * once a buffer frame is allocated call file->readPage() to place the page in the frame
       * insert the page into the hash table and call Set() to set the page properly and return a 
       * pointer to the frame where the page is pinned
       */
    	BufMgr::allocBuf(this->clockHand); //allocate the buffer frame pointed to by clockHand for the page
    	*page = file->readPage(pageNo); //Read the page in from memory
    	hashTable->insert(file, pageNo,this->clockHand); //place the page into the buffer frame	
    	bufDescTable[this->clockHand].Set(file, pageNo); //Call to set the BufDesc properly	
    }
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{

  // decrement pinCnt of the frame containing (file, pageNo)
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

  }
}

void BufMgr::flushFile(const File* file) 
{
  // scan bufDesc Table for pages belonging to file
  for (FrameId i = 0; i < this->numBufs-1; i++)
  {
    if (bufDescTable[i].file == file)
    {
      //found page of the given file
      if (bufDescTable[i].pinCnt > 0)
        throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
      if (bufDescTable[i].valid == false)
        throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
      if (bufDescTable[i].dirty)
      {
        bufDescTable[i].file->writePage(this->bufPool[i]);
        bufDescTable[i].dirty = false;
      }
      this->hashTable->remove(file, bufDescTable[i].pageNo);
      bufDescTable[i].Clear();
    }
  }

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
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
