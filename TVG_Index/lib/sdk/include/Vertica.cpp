/* Copyright (c) 2005 - 2014, Hewlett-Packard Development Co., L.P.  -*- C++ -*- 
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of the Hewlett-Packard Company nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ****************************/
/*
 *
 * Description: Support code for UDx subsystem
 *
 * Create Date: Feb 10, 2011
 */
#include <Vertica.h>
#include <BuildInfo.h>
#include <VerticaUDFSImpl.h>
#include <VerticaDFS.h>

namespace Vertica {

extern "C" {
    VT_THROW_EXCEPTION vt_throw_exception = 0; // function pointer to throw exception
    ServerFunctions *VERTICA_INTERNAL_fns = NULL;
    VT_THROW_INTERNAL_EXCEPTION vt_throw_internal_exception = 0; // function pointer to throw exception
    VT_ERRMSG vt_server_errmsg = 0;
}

// setup global function pointers, called once the UDx library is loaded
extern "C" void setup_global_function_pointers(VT_THROW_EXCEPTION throw_exception, VT_THROW_INTERNAL_EXCEPTION throw_internal_exception, VT_ERRMSG server_errmsg, ServerFunctions *fns)
{
    vt_throw_exception = throw_exception;
    vt_throw_internal_exception = throw_internal_exception;
    vt_server_errmsg = server_errmsg;
    VERTICA_INTERNAL_fns = fns;
}

extern "C" void get_vertica_build_info(VerticaBuildInfo &vbi)
{
    vbi.brand_name    = VERTICA_BUILD_ID_Brand_Name;
    vbi.brand_version = VERTICA_BUILD_ID_Brand_Version;
    vbi.sdk_version   = VERTICA_BUILD_ID_SDK_Version;
    vbi.codename      = VERTICA_BUILD_ID_Codename;
    vbi.build_date    = VERTICA_BUILD_ID_Date;
    vbi.build_machine = VERTICA_BUILD_ID_Machine;
    vbi.branch        = VERTICA_BUILD_ID_Branch;
    vbi.revision      = VERTICA_BUILD_ID_Revision;
    vbi.checksum      = VERTICA_BUILD_ID_Checksum;
}

// This big enough for anything reasonable
#define MSG_BUF 2048

int makeErrMsg(std::basic_ostream<char, std::char_traits<char> > &err_msg, const char *fmt, ...)
{
    va_list va;
    char msg[MSG_BUF];

    va_start(va, fmt);
    vsnprintf(msg, MSG_BUF, gettext(fmt), va);
    va_end(va);

    err_msg << msg;
    return 0;
}

void dummy() {}


// Global Library Manifest object & getter function
UserLibraryManifest &GlobalLibraryManifest()
{
    static UserLibraryManifest singleton;
    return singleton;
}

extern "C" UserLibraryManifest* get_library_manifest()
{
    return &GlobalLibraryManifest();
}

// Date/Time-parsing functions
DateADT dateIn(const char *str, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateIn(str, report_error);
}

TimeADT timeIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeIn(str, typmod, report_error);
}

TimeTzADT timetzIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzIn(str, typmod, report_error);
}

Timestamp timestampIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampIn(str, typmod, report_error);
}

TimestampTz timestamptzIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzIn(str, typmod, report_error);
}

Interval intervalIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->intervalIn(str, typmod, report_error);
}

DateADT dateInFormatted(const char *str, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateInFormatted(str, format, report_error);
}

TimeADT timeInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeInFormatted(str, typmod, format, report_error);
}

TimeTzADT timetzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzInFormatted(str, typmod, format, report_error);
}

Timestamp timestampInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampInFormatted(str, typmod, format, report_error);
}

TimestampTz timestamptzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzInFormatted(str, typmod, format, report_error);
}

Oid getUDTypeOid(const char *typeName) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->getUDTypeOid(typeName);
}

Oid getUDTypeUnderlyingOid(Oid typeOid) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->getUDTypeUnderlyingOid(typeOid);
}

bool isUDType(Oid typeOid) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->isUDType(typeOid);
}

void log(const char *format, ...)
{
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    va_list ap;
    va_start(ap, format);
    VERTICA_INTERNAL_fns->log(format, ap);
    va_end(ap);
}

vint getUDxDebugLogLevel()
{
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->getUDxDebugLogLevel();
}

void AggregateFunction::updateCols(BlockReader &arg_reader, char *arg, int count,
                                   IntermediateAggs &intAggs, char *aggPtr,
                                   std::vector<int> &intOffsets) {
    int colstride = (arg_reader.colstrides.empty() ? 0 : arg_reader.colstrides[0]);
    arg_reader.cols[0] = arg + (arg_reader.indices ? colstride*arg_reader.indices[0] : 0);
    arg_reader.count = count;
    arg_reader.index = 0;

    for (uint i=0; i<intOffsets.size(); ++i)
        intAggs.cols[i] = aggPtr + intOffsets[i];
}

// Defined outside the class to get the dependency order right with VerticaType
bool VNumeric::charToNumeric(const char *str, const VerticaType &type, VNumeric &target) {
    uint64 *tmpStorage = (uint64*)alloca(target.nwds*sizeof(uint64) * 2); // Extra factor of 2 just for good measure
    VNumeric tmpVal(tmpStorage, target.typmod);
    int64 *pout = (int64*) tmpVal.words;

    int p, s;
    if (!Basics::BigInt::charToNumeric(str, strlen(str), true,
            pout, target.nwds, p, s, true)) {
        return false;
    }
    if (!Basics::BigInt::rescaleNumeric(target.words,
            type.getNumericLength() >> 3, Basics::getNumericPrecision(
                    target.typmod), Basics::getNumericScale(target.typmod),
            pout, target.nwds, p, s)) {
        return false;
    }

    return true;
}
 

// If you modify this class, do same modifications to VerticaSDK/com/vertica/sdk/DFSFile.java
void DFSFile::create(DFSScope dfsScope, DFSDistribution dfsDistrib)
{
    validateFileOrThrow();
    scope = dfsScope;
    dist = dfsDistrib;
    status = WRITE_CREATED;
	isFfile = true;
    fileWriter = fileManager->openForWrite(fileName, scope, dist); 
}

/**
* Deletes a DFS file.
* @returns 0 if successful, throw exceptions if there are errors.
**/
int DFSFile::deleteIt(bool isRecursively)
{
    validateFileOrThrow();
    // Directory deletes are not yet supported
    if (!isFfile) ereport(ERROR,(errmsg("Directory deletes are not supported through API. Use dfs_delete() meta function")));
    fileManager->deleteIt(fileName, isRecursively);
    isExists = false;
    return 0;
}
  
/**
* Lists file under the path specified by 'fileName'
* @returns a list of DFSFile found under the path.
**/
void DFSFile::listFiles(std::vector<Vertica::DFSFile> &fileList)
{
    if (isFfile) 
    { 
        ereport(ERROR,(errmsg("File path [%s] is not a directory. File listings are only permitted on directories", 
                            fileName.c_str())));
    }
    fileManager->listFiles(fileName, fileList);
}

/**
* Renames file identified by 'srcFilePath' to 'destFilePath'
* returns 0, throws exceptions if there are errors.
int DFSFile::rename(std::string newName)
{
    validateFileOrThrow();
    if (!isExists) ereport(ERROR, (errmsg("Source file/directory does not exists")));
    fileName = fileManager->rename(fileName, newName);
    return 0;
}
**/

/**
* Copy a file/directory from 'srcFilePath' to 'destFilePath'.
* returns 0, throws exceptions if there are errors.
int DFSFile::copy(DFSFile &dfsFile, bool isRecursively)
{
    validateFileOrThrow();
    return fileManager->copy(fileName, dfsFile.getName(), isRecursively);
}
**/

/**
* Make a directory, identified by 'dirPath'
* returns 0, throws exceptions if there are errors.
int DFSFile::makeDir()
{
    validateFileOrThrow();
    return fileManager->makeDir(fileName);
}
**/

void DFSFile::setName(std::string fName) 
{
    fileName = fName;
    init();
}

bool DFSFile::exists()
{
    validateFileOrThrow();
    return isExists;
}  

/**
* Do basic validations such as emptiness of file path
* and existence of a pointer to FileManager instance. 
**/
void DFSFile::validateFileOrThrow()
{
    if (fileName.empty())
    {
        ereport(ERROR,(errmsg("File path [%s] is not set/ empty.", 
                                            fileName.c_str())));
    }
    if (fileManager == NULL)
    {
        ereport(ERROR,(errmsg("DFSFile has not initiated properly"))); 
    } 
}
 /**
* Initiates the DFS file by validating given path against 
* existing DFS.
*/
void DFSFile::init()
{
    validateFileOrThrow();
    isExists = fileManager->initDFSFile(*this);
}

/**
  * DFSFileReader
  * If you modify this class, do same modifications to VerticaSDK/com/vertica/sdk/DFSFileReader.java
**/
void DFSFileReader::open()
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileReader is not initialized, cannot open for reading")));

    if (isOpenForRead)  return;
    fileReader = fileManager->openForRead(fileName);
    isOpenForRead = true;
}

/**
* Reads 'size' of bytes into buffer pointed by 'ptr' from the file 
* opened for reading.
* @return number of bytes read, 0 if no bytes were read, indicates the EOF.
* throws exceptions if there are errors.
**/
size_t DFSFileReader::read(void* ptr, size_t size)
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileReader is not initialized, cannot use for reading")));
    if (!isOpenForRead)
    {
        ereport(ERROR,(errmsg("File is not opened for reading.")));
    }
    return fileManager->read(fileReader, ptr, size); 
}

/*
* Set the read position to a new place within the file stream.
* @offset Number of bytes to offset from @whence
* @whence Values could be SEEK_SET (0 - begining of the file), SEEK_CUR(1 - current position of the file pointer), 
* SEEK_END(2- End of the file)
* @return resulting offset position as measured in bytes from begining of the file, throw an exception on error.
*/
off_t DFSFileReader::seek(off_t offset, int whence)
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileReader is not initialized, cannot use for seeking")));
    if (offset > (off_t)fsize)
    {
        ereport(ERROR,(errmsg("Specified offset [%jd] is larger than file size.", offset)));
    }
    if (!isOpenForRead)
    {
        ereport(ERROR,(errmsg("File is not opened for reading.")));
    }
    return fileManager->seek(fileReader, offset, whence);
}

/**
* Closes the file opened for reading. 
**/
void DFSFileReader::close()
{
    if (!(fileManager))
        return; // There is no reason to throw an error if there is nothing to close.
    if (isOpenForRead) 
    {
        fileManager->closeReader(fileReader);
        isOpenForRead = false;
        fileReader = 0;
    }
}


//  DFSFileWriter
/**
* Opens a file for writing. 
* If you modify this class, do same modifications to VerticaSDK/com/vertica/sdk/DFSFileReader.java
**/
void DFSFileWriter::open()
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileWriter is not initialized, cannot open for writing")));
    if (isOpenForWrite)  return;
    if (!(fileWriter)) fileWriter = fileManager->openForWrite(
                    fileName, scope, dist); 
    isOpenForWrite = true;
}

/**
* Writes 'size' of bytes into the file identified by 'writerID' from the
* buffer pointed by 'ptr'.
* @return number of bytes written, less than 0 if there are any errors.
**/
size_t DFSFileWriter::write(const void* ptr, size_t size)
{
    if (!(fileManager))
        ereport(ERROR,(errmsg("DFSFileWriter is not initialized, cannot use for writing")));
    if (!isOpenForWrite)
    {
        ereport(ERROR,(errmsg("File is not opened for writing.")));
    }
    return fileManager->write(fileWriter, ptr, size); 
}

/**
* Closes the file opened for writing. 
**/
void DFSFileWriter::close()
{
    if (!(fileManager))
        return; // There is no reason to throw an error if there is nothing to close.
    if (isOpenForWrite) 
    {
        fileManager->closeWriter(fileWriter);
        isOpenForWrite = false;
    }
}

} // namespace Vertica

namespace Basics {

// BigInt (Numeric) functions
// Simple ones are provided below for performance reasons.

void BigInt::setZero(void *buf, int nWords){
    int64 * ibuf = static_cast<int64 *>(buf);
    for (int i=0; i<nWords; ++i) ibuf[i] = 0;
}

bool BigInt::isZero(const void *buf, int nWords){
   const int64 *ibuf = static_cast<const int64 *>(buf);
   int64 res = 0;
   for (int i=0; i<nWords; ++i) {
       res |= ibuf[i];
   }
   return res == 0;
}

void BigInt::setNull(void *buf, int nWords){
    int64 *ibuf = static_cast<int64 *>(buf);
    Assert (nWords > 0);
    ibuf[0] = vint_null;
    for (int i=1; i<nWords; ++i) ibuf[i] = 0;
}

bool BigInt::isNull(const void *buf, int nWords){
   const int64 *ibuf = static_cast<const int64 *>(buf);
   Assert (nWords > 0);
   if (ibuf[0] != vint_null) return false;
   int64 res = 0;
   for (int i=1; i<nWords; ++i) {
       res |= ibuf[i];
   }
   return res == 0;
}

bool BigInt::isEqualNN(const void *buf1, const void *buf2, int nWords){
   const int64 *ibuf1 = static_cast<const int64 *>(buf1);
   const int64 *ibuf2 = static_cast<const int64 *>(buf2);
   int64 res = 0;
   for (int i=0; i<nWords; ++i) {
       res |= ibuf1[i] - ibuf2[i];
   }
   return res == 0;
}

int BigInt::compareNN(const void *buf1, const void *buf2, int nWords){
   Assert (nWords > 0);
   const int64 *ibuf1 = static_cast<const int64 *>(buf1);
   const int64 *ibuf2 = static_cast<const int64 *>(buf2);
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);

   if (ibuf1[0] != ibuf2[0]) return ibuf1[0] < ibuf2[0] ? -1 : 1;

   for (int i=1; i<nWords; ++i) {
       if (ubuf1[i] != ubuf2[i]) return ubuf1[i] < ubuf2[i] ? -1 : 1;
   }
   return 0; // ==
}

void BigInt::incrementNN(void *buf, int nWords) {
    uint64 *ubuf = static_cast<uint64 *>(buf);
    uint64 cf = 1;
    int i = nWords;
    while (cf && i>0) {
        --i;
        ubuf[i] += cf;
        cf = (ubuf[i] == 0);
    }
}

void BigInt::invertSign(void *buf, int nWords) {
   uint64 *ubuf = static_cast<uint64 *>(buf);
   for (int i=0; i<nWords; ++i) ubuf[i] ^= 0xFFFFFFFFFFFFFFFFLLU;
   incrementNN(ubuf, nWords);
}

uint64 BigInt::addNN(void *outBuf, const void *buf1, const void *buf2, int nWords) {
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);
   uint64 *ubufo = static_cast<uint64 *>(outBuf);

   uint64 cf = 0;
   for (int i=nWords-1; i>=0; --i)
   {
       uint64 ncf = 0;
       uint64 rw = ubuf1[i]+ubuf2[i];
       if (ubuf1[i] > rw) ++ncf;
       uint64 rw2 = rw+cf;
       if (rw > rw2) ++ncf;
       ubufo[i] = rw2;
       cf = ncf;
   }
   return cf;
}

uint64 BigInt::accumulateNN(void *outBuf, const void *buf1, int nWords) {
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   uint64 *ubufo = static_cast<uint64 *>(outBuf);

   uint64 cf = 0;
   for (int i=nWords-1; i>=0; --i)
   {
       uint64 ncf = 0;
       uint64 rw = ubuf1[i]+ubufo[i];
       if (ubufo[i] > rw) ++ncf;
       uint64 rw2 = rw+cf;
       if (rw > rw2) ++ncf;
       ubufo[i] = rw2;
       cf = ncf;
   }
   return cf;
}

/** 
 * \brief Subtract 2 numbers w/ same number of digits
 *  No null handling
 */
void BigInt::subNN(void *outBuf, const void *buf1, const void *buf2, int nWords) {
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);
   uint64 *ubufo = static_cast<uint64 *>(outBuf);

   uint64 cf = 0;
   for (int i=nWords-1; i>=0; --i)
   {
       uint64 ncf = 0;
       uint64 rw = ubuf1[i]-ubuf2[i];
       if (ubuf1[i] < rw) --ncf;
       uint64 rw2 = rw+cf;
       if (rw < rw2) --ncf;
       ubufo[i] = rw2;
       cf = ncf;
   }
}

void BigInt::shiftRightNN(void *buf, unsigned nw, unsigned bitsToShift)
{
    Assert(nw > bitsToShift/64);
    uint64 *bbuf = static_cast<uint64 *>(buf);
    unsigned from = nw - (bitsToShift / 64) - 1, to = nw - 1;
    unsigned shift = bitsToShift % 64;
    if (shift==0) {
        for ( ; from > 0; --from) {
            bbuf[to--] = bbuf[from];
        }
        bbuf[to] = bbuf[from];
    } else {
        for ( ; from > 0; --from) {
            bbuf[to--] = (bbuf[from-1] << (64 - shift)) | (bbuf[from] >> shift);
        }
        bbuf[to] = bbuf[from] >> shift;
    }
    while (to > 0) bbuf[--to] = 0;
}

void BigInt::shiftLeftNN(void *buf, unsigned nw, unsigned bitsToShift)
{
    Assert(nw > bitsToShift/64);
    uint64 *bbuf = static_cast<uint64 *>(buf);
    unsigned from = bitsToShift / 64, to = 0;
    unsigned shift = bitsToShift % 64;
    if (shift==0) {
        for ( ; from < nw-1; ++from) {
            bbuf[to++] = bbuf[from];
        }
        bbuf[to] = bbuf[from];
    } else {
        for ( ; from < nw-1; ++from) {
            bbuf[to++] = (bbuf[from] << shift) | (bbuf[from+1] >> (64 - shift));
        }
        bbuf[to] = bbuf[from] << shift;
    }
    while (to < nw-1) bbuf[++to] = 0;
}

void BigInt::fromIntNN(void *buf, int nWords, int64 val)
{
   uint64 *ubufo = static_cast<uint64 *>(buf);
   Assert (nWords > 0);

   ubufo[nWords - 1] = uint64(val);
   // Sign extend
   uint64 ext = (val & vint_null) ? -1LLU : 0;
   for (int i=0; i<nWords - 1; ++i) ubufo[i] = ext;
}

bool BigInt::toIntNN(int64 &out, const void *buf, int nWords)
{
   const int64 *ibuf = static_cast<const int64 *>(buf);
   Assert (nWords > 0);
   out = ibuf[nWords-1];
   if (out & vint_null) {
       for (int i=0; i<nWords - 1; ++i) {
           if (ibuf[i] != -1LL) return false;
       }
   }
   else {
       for (int i=0; i<nWords - 1; ++i) {
           if (ibuf[i] != 0) return false;
       }
   }
   return true;
}

int BigInt::usedWordsUnsigned(const void *buf, int nWords)
{
    const uint64 *ubuf = static_cast<const uint64 *>(buf);
    int unusedWords = 0;
    for (; (unusedWords < nWords) && !ubuf[unusedWords]; ++unusedWords) ;
    return nWords - unusedWords;
}

void BigInt::mulUnsignedN1(void *obuf, const void *ubuf, int uw, uint64 v)
{
    const uint64 *u = static_cast<const uint64 *>(ubuf);
    uint64 *o = static_cast<uint64 *>(obuf);

    Multiply(u[uw-1], v, o[uw-1], o[uw]); // N-by-1 multiply
    for (int i = uw-2; i >= 0; --i) {
        uint64 lo;
        Multiply(u[i], v, o[i], lo);
        o[i+1] += lo;
        if (o[i+1] < lo) o[i]++;        // can't overflow
    }
}

void BigInt::mulUnsignedNN(void *obuf, const void *buf1, int nw1,
                           const void *buf2, int nw2)
{
    const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
    const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);
    uint64 *ubufo = static_cast<uint64 *>(obuf);

    int nwo = nw1+nw2;
    setZero(obuf, nwo);

    Assert (nw1>0);
    Assert (nw2>0);

    // Optimize out leading and trailing zeros
    int s1 = 0, e1 = nw1-1;
    for (; (s1 < nw1) && !ubuf1[s1]; ++s1) ;
    for (; (e1 >= 0) && !ubuf1[e1]; --e1) ;

    // Now multiply
    for (int i2=nw2-1; i2 >= 0; --i2)
    {
        if (!ubuf2[i2]) continue; // Optimize out extra 0s

        for (int i1 = e1; i1>= s1; --i1)
        {
            uint64 rl;
            uint64 rh;

            Multiply(ubuf1[i1], ubuf2[i2], rh, rl);

            // Add to the result (w/carry)
            int idx = i1 + i2;
            ubufo[idx+1] += rl;
            if (ubufo[idx+1] < rl) ++rh; // can't overflow
            ubufo[idx] += rh;
            uint64 cf = (ubufo[idx] < rh);

            // Propagate carry
            while (cf) {
                ubufo[--idx] += cf;
                cf = (ubufo[idx] == 0);
            }
        }
    }
}

uint64 BigInt::divUnsignedN1(void *qbuf, int qw, int round,
                             const void *ubuf, int uw, uint64 v)
{
    if (v == 0) {
        if (isZero(ubuf, uw)) {
            setZero(qbuf, qw);
            return 0;                   // 0/0 -> 0,0
        }
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                        errmsg("division by zero")));
    }
    Assert(qw > 0);
    Assert(uw > 0);
    const uint64 *ub = static_cast<const uint64 *>(ubuf);
    uint64 *qb = static_cast<uint64 *>(qbuf);

    if (qb == ub)
        Assert(qw == uw);
    else
        Basics::BigInt::setZero(qb, qw-uw);

    uint64 rem = 0, q, r;
    for (int j = 0; j < uw; ++j) {
        Divide(rem, ub[j], v, q, r);
        rem = r;
        int k = j + qw - uw;
        if (k < 0)
            Assert(q == 0);
        else
            qb[k] = q;
    }

    if (round && rem >= v - rem) // rem >= v / 2
        Basics::BigInt::incrementNN(qb, qw);
    return rem;
}

void BigInt::divUnsignedNN(void *qbuf, int qw, int round,
                           uint64 *rbuf, int rw, const void *ubuf,
                           int uw, const void *vbuf, int vw)
{
    Assert(qw > 0);
    Assert(uw > 0);
    Assert(vw > 0);

    if (rbuf)
        Basics::BigInt::setZero(rbuf, rw);

    // Optimize out leading zeros from v
    const uint64 *vb = static_cast<const uint64 *>(vbuf);
    int vs = 0;
    while (vb[vs] == 0)
        if (++vs == vw) {
            if (isZero(ubuf, uw)) {
                setZero(qbuf, qw);
                return;                 // 0/0 -> 0,0
            }
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("division by zero")));
        }
    int n = vw - vs;                    // length of v in words

    // Optimize out leading zeros from u
    const uint64 *ub = static_cast<const uint64 *>(ubuf);
    int us = 0;
    while (ub[us] == 0)
        if (++us == uw) {
            Basics::BigInt::setZero(qbuf, qw);
            return;                     // u == 0, so qbuf and rbuf = 0
        }
    int ul = uw - us;                   // length of u in words (m+n)

    uint64 *qb = static_cast<uint64 *>(qbuf);

    // Optimize N by 1 case
    if (n == 1) {
        uint64 rem = divUnsignedN1(qb, qw, round, ub+us, ul, vb[vw-1]);
        if (rbuf)
            rbuf[rw-1] = rem;
        return;
    }

    // D1 Normalize
    uint64 q, r;
    uint64 v[n+2];                      // v is v[1..n] scaled by d
    v[0] = v[n+1] = 0;                  // v[n+1] is a hack; verify if it works!
    Basics::BigInt::copy(v+1, vb+vs, n);
    uint64 d = __builtin_clzll(v[1]);   // count leading zero bits
    Basics::BigInt::shiftLeftNN(v+1, n, d);

    int m = ul - n;

    uint64 u[ul+1];                     // u is u[0..ul] scaled by d
    u[0] = 0;
    Basics::BigInt::copy(u+1, ub+us, ul);
    Basics::BigInt::shiftLeftNN(u, ul+1, d);

    Basics::BigInt::setZero(qb, qw);

    // D2 Loop for each q digit
    for (int j = 0; j <= m; ++j) {
        // D3 calculate qhat and rhat
        if (u[j] == v[1]) {             // Divide will overflow
            q = 0;
            r = u[j+1];
            goto tag4H;
        }
        Divide(u[j],u[j+1], v[1], q, r);

        while (true) {
            uint64 hi, lo;
            Multiply(v[2], q, hi, lo);  // hack if vl==1
            if (hi < r ||
                (hi == r && lo <= u[j+2]))
                break;                  // q is no more than 1 too high
        tag4H:
            --q;
            r += v[1];
            if (r < v[1])               // 056: overflow, quit
                break;
        }

        uint64 t[n+1];                  // D4 multiply and subtract
        mulUnsignedN1(t, v+1, n, q);
        Basics::BigInt::subNN(u+j, u+j, t, n+1);

        if (u[j] != 0) {                // D5 test remainder
            Basics::BigInt::addNN(u+j, u+j, v, n+1); // D6 add back
            --q;
            Assert(u[j] == 0);
        }
        if (q != 0) {
            int k = j - m + qw - 1;
            Assert(k >= 0 && k < qw);
            qb[k] = q;
        }
    }

    if (rbuf) {
        int i = Min(ul, n);
        Assert(rw >= i);                              // rbuf overflow
        Basics::BigInt::copy(rbuf+rw-i, u+ul+1-i, i); // D8 unnormalize
        Basics::BigInt::shiftRightNN(rbuf+rw-i, i, d);
    }

    if (round && m >= 0) {
        Basics::BigInt::shiftLeftNN(u+m, n+1, 1);        // double r
        if (Basics::BigInt::compareNN(u+m, v, n+1) >= 0) // if r >= v/2
            Basics::BigInt::incrementNN(qb, qw);
    }
    return;
}

long double BigInt::convertPosToFloatNN(const void *bbuf, int bwords)
{
    const uint64 *buf = static_cast<const uint64 *>(bbuf);

    // Count leading zero bits
    int zbits = 0;
    int i; // Will index the first nonzero word
    for (i=0; i<bwords; ++i) {
        if (buf[i] != 0) {
            zbits += __builtin_clzll(buf[i]);
            break;
        }
        zbits += 64;
    }
    int tbits = bwords * 64;
    if (zbits == tbits) return 0.0L; // Is zero, let's not confuse ourselves.

    // Pick out mantissa
    uint64 m = buf[i] << (zbits & 63);
    ++i;
    if ((zbits & 63) && i != bwords) {
         m |= (buf[i]) >> (64 - (zbits & 63));
    }

    // Build resulting float
    long_double_parts ldp;
    ldp.mantissa = m;
    ldp.exponent = tbits - zbits + 16382; // 1 nonzero bit = 2^0 = 16383 exponent

    return ldp.value;
}

ifloat BigInt::numericToFloat(const void *buf, int nwds, ifloat tenthtoscale)
{
    if (Basics::BigInt::isNull(buf, nwds)) {
        return vfloat_null;
    }
    else {
        bool isNeg = false;
        uint64 tmp[nwds];
        if (Basics::BigInt::isNeg(buf, nwds)) {
            Basics::BigInt::copy(tmp, buf, nwds);
            Basics::BigInt::invertSign(tmp, nwds);
            buf = tmp;
            isNeg = true;
        }
        long double rv = Basics::BigInt::convertPosToFloatNN(buf, nwds)
            * tenthtoscale;
        return isNeg ? -rv : rv;
    }
}

void BigInt::numericMultiply(const uint64 *pa, int nwdsa,
                            const uint64 *pb, int nwdsb,
                            uint64 *outNum, int nwdso)
{
    if (nwdsa + nwdsb == nwdso)
    {
        if (Basics::BigInt::isNull(pa, nwdsa) ||
            Basics::BigInt::isNull(pb, nwdsb))
        {
            Basics::BigInt::setNull(outNum, nwdso);
        }
        else
        {
            // Multiply unsigned
            uint64 wdsa[nwdsa], wdsb[nwdsb]; // For sign inversions if needed
            const void *pva = pa, *pvb = pb;
            int negs = 0;
            if (Basics::BigInt::isNeg(pa, nwdsa)) {
                ++negs;
                pva = wdsa;
                Basics::BigInt::copy(wdsa, pa, nwdsa);
                Basics::BigInt::invertSign(wdsa, nwdsa);
            }
            if (Basics::BigInt::isNeg(pb, nwdsb)) {
                ++negs;
                pvb = wdsb;
                Basics::BigInt::copy(wdsb, pb, nwdsb);
                Basics::BigInt::invertSign(wdsb, nwdsb);
            }
            Basics::BigInt::mulUnsignedNN(outNum, pva, nwdsa, pvb, nwdsb);
            if (negs == 1) Basics::BigInt::invertSign(outNum, nwdso);
        }
    }
    else
    {
        if (Basics::BigInt::isNull(pa, nwdsa) ||
            Basics::BigInt::isNull(pb, nwdsb))
        {
            Basics::BigInt::setNull(outNum, nwdso);
        }
        else
        {
            // Multiply unsigned...
            uint64 otemp[nwdsa+nwdsb]; // Result has to go in temp place
            uint64 wdsa[nwdsa], wdsb[nwdsb]; // For sign inversions if needed
            const void *pva = pa, *pvb = pb;
            int negs = 0;
            if (Basics::BigInt::isNeg(pa, nwdsa)) {
                ++negs;
                pva = wdsa;
                Basics::BigInt::copy(wdsa, pa, nwdsa);
                Basics::BigInt::invertSign(wdsa, nwdsa);
            }
            if (Basics::BigInt::isNeg(pb, nwdsb)) {
                ++negs;
                pvb = wdsb;
                Basics::BigInt::copy(wdsb, pb, nwdsb);
                Basics::BigInt::invertSign(wdsb, nwdsb);
            }
            Basics::BigInt::mulUnsignedNN(otemp, pva, nwdsa, pvb, nwdsb);
            Basics::BigInt::copy(outNum, otemp+(nwdsa+nwdsb-nwdso), nwdso);
            if (negs == 1) Basics::BigInt::invertSign(outNum, nwdso);
        }
    }
}

// More BigInt (Numeric) functions
//
// More-complex ones call back into the server; the extra
// per-call overhead is small because the functions are
// more expensive, and this keeps the SDK lightweight.
//
// The implementations of many of these functions were
// provided in previous SDK versions.  If they are needed,
// they are still available as part of the fenced-mode
// source code package.  But virtually all uses should
// simply use the provided callbacks.

void BigInt::setNumericBoundsFromType(uint64 *numUpperBound,
                                     uint64 *numLowerBound, 
                                     int nwdso, int32 typmod) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    VERTICA_INTERNAL_fns->setNumericBoundsFromType(numUpperBound,
                                                   numLowerBound,
                                                   nwdso,
                                                   typmod);
}

bool BigInt::checkOverflowNN(const void *po, int nwdso, int32 typmodo) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->checkOverflowNN(po, nwdso, typmodo);
}

bool BigInt::checkOverflowNN(const void *po, int nwo, int nwdso, int32 typmodo) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->checkOverflowNN(po, nwo, nwdso, typmodo);
}

void BigInt::NumericRescaleDown(uint64 *wordso, int nwdso, int32 typmodo,
                                uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->NumericRescaleDown(wordso, nwdso, typmodo,
                                                    wordsi, nwdsi, typmodi);
}

void BigInt::NumericRescaleUp(uint64 *wordso, int nwdso, int32 typmodo,
                              uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->NumericRescaleUp(wordso, nwdso, typmodo,
                                                  wordsi, nwdsi, typmodi);
}

void BigInt::NumericRescaleSameScaleSmallerPrec(uint64 *wordso, int nwdso, int32 typmodo,
                                                uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->NumericRescaleSameScaleSmallerPrec(wordso, nwdso, typmodo,
                                                                    wordsi, nwdsi, typmodi);
}

void BigInt::castNumeric(uint64 *wordso, int nwdso, int32 typmodo,
                        uint64 *wordsi, int nwdsi, int32 typmodi) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->castNumeric(wordso, nwdso, typmodo,
                                             wordsi, nwdsi, typmodi);
}

bool BigInt::rescaleNumeric(void *out, int ow, int pout, int sout,
                            void *in, int iw, int pin, int sin) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->rescaleNumeric(out, ow, pout, sout,
                                                in, iw, pin, sin);
}

void BigInt::numericDivide(const uint64 *pa, int nwdsa, int32 typmoda,
                          const uint64 *pb, int nwdsb, int32 typmodb,
                          uint64 *outNum, int nwdso, int32 typmodo) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->numericDivide(pa, nwdsa, typmoda,
                                               pb, nwdsb, typmodb,
                                               outNum, nwdso, typmodo);
}

bool BigInt::setFromFloat(void *bbuf, int bwords, int typmod,
                          long double value, bool round) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->setFromFloat(bbuf, bwords, typmod, value, round);
}

bool BigInt::charToNumeric(const char *c, int clen, bool allowE,
                           int64 *&pout, int &outWords,
                           int &precis, int &scale, bool packInteger /* = false */) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->charToNumeric(c, clen, allowE,
                                               pout, outWords,
                                               precis, scale, packInteger);
}

void BigInt::binaryToDecScale(const void *bbuf, int bwords, char *outBuf, int olen, int scale) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->binaryToDecScale(bbuf, bwords, outBuf, olen, scale);
}

} // namespace Basics

















