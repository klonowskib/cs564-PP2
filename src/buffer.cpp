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
}

void BufMgr::advanceClock()
{	
    if (clockHand < numBufs - 1) 
	clockHand++; ///Increment clockHand if it is less than the highest buffer page index.
    else if (clockHand == numBufs - 1)
	clockHand = 0;	///If equal to the highest buffer frame index, set clockHand to 0.
}

void BufMgr::allocBuf(FrameId & frame) 
{

}
	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    ///Case 1: Page is NOT in the buffer pool.
    /**
     * If the page is not in the buffer pool, call allocBuff() method
     * once a buffer frame is allocated call file->readPage() to place the page in the frame
     * insert the page into the hash table and call Set() to set the page properly and return a 
     * pointer to the frame where the page is pinned
     */ 
    FrameId frameNo = this->numBufs; //Will hold the frame number if the page is in the pool
    hashTable->lookup(file, pageNo, frameNo); // Look for the page in the pool
    if (frameNo <= this->numBufs-1) // condition to check if a page is in the pool
    {   
	BufMgr::allocBuf(this->clockHand); //allocate the buffer frame pointed to by clockHand for the page
	*page = file->readPage(pageNo); //Read the page in from memory
	hashTable->insert(file, pageNo,this->clockHand); //place the page into the buffer frame	
	bufDescTable->Set(file, pageNo); //Call to set the BufDesc properly	
	return; 
    }
    ///Case 2: Page is in the buffer pool.
    /**
     * Update the refbit and increment the pin count
     * return a pointer to the buffer frame that references the page
     */
    else 
    {
	bufDescTable->refbit = true;
	bufDescTable->pinCnt++;
	page = &bufPool[frameNo];	
    }
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
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
