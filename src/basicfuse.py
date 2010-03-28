#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT, EISDIR
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from hashlib import sha512
from fuse import FUSE, Operations, LoggingMixIn
import socket
from  connectlib import getblock, putblock, rmblock
import simplejson

#TODO symlinking doesn't work (soft)
#TODO HARDLINKING isn't supported
#there is some odd issue with vim with a small blocksize

BLOCKSIZE = 8096
BLOCKNAMESIZE = 32

def blockexists(key):
    return getblock(key) != ""

def exists(path):
    return blockexists('METADATA=' + path)

def getmetadata(path):
    pth = 'METADATA=' + path
#    print "Finding metadata for", path
    if not blockexists(pth):
#        print "No metadata for ", path, "using default"
        return dict(st_size=0, blocks=[], xattr={}, files=[], st_mode=(S_IFDIR | 0755),  st_ctime=time(), st_mtime=time(), st_atime=time(), st_nlink=2)
    return simplejson.loads(getblock(pth))

def setmetadata(path, meta):
#    print "Setting", path, "metadata to", meta
    putblock('METADATA=' + path,  simplejson.dumps(meta))

def getparent(path):
    parent = "/".join(path.split("/")[:-1])
    if parent == "":
        parent = "/"
    return parent

#def getblock(name):
#    print "Getting block", name
#    return simplejson.loads(connectlib.getblock(name))

#def putblock(name, value):
#    print "Putting block",repr(name), repr(value)
#    connectlib.putblock(name, simplejson.dumps(value))
#def getblock(name):
#    return blocks[name]

#def putblock(name, block):
#    global blocks
#    blocks[name] = block

def nameblock(block):
    return sha512(block).hexdigest()[:BLOCKNAMESIZE]

#def rmblock(name):
#    print "removing block", name
#    del blocks[name]

class BasicFuse(LoggingMixIn, Operations):
    """Basic orion implementation over fuse"""
    
    def __init__(self):
        self.files = {}
        self.data = defaultdict(str)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            st_mtime=now, st_atime=now, st_nlink=2)
        
    def chmod(self, path, mode):
        meta = getmetadata(path)
        meta['st_mode'] &= 0770000
        meta['st_mode'] |= mode
        setmetadata(path, meta)
        return 0

    def chown(self, path, uid, gid):
        meta = getmetadata(path)
        meta['st_uid'] = uid
        meta['st_gid'] = gid
        setmetadata(path, meta)

    def create(self, path, mode):
        setmetadata(path, dict(blocks=[], xattr={}, st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time()))

        parent = getparent(path) #update file list
        parmeta = getmetadata(parent)
        parmeta['files'] += [path]
        setmetadata(parent, parmeta)

        self.fd += 1
        return self.fd
    
    def getattr(self, path, fh=None):
        if not exists(path):
            raise OSError(ENOENT, '')
        st = getmetadata(path)
        return st
    
    def getxattr(self, path, name, position=0):
        meta = getmetadata(path)
        return meta['xattr'].get(name, " ")

    
    def listxattr(self, path):
        meta = getmetadata(path)
        return meta['xattr'].keys()
    
    def mkdir(self, path, mode):
        meta = dict(xattr={}, st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(), files=[])
        setmetadata(path, meta)
        parent = getparent(path)
        parmeta = getmetadata(parent)
        parmeta['st_nlink'] += 1
        parmeta['files'] += [path]

        setmetadata(parent, parmeta)

    def open(self, path, flags):
        self.fd += 1
        return self.fd
    
    def read(self, path, size, offset, fh):
#        print "Reading", path, size, offset, fh
        #in short we can't return less then what was asked for unless there honestly isn't anything left
        #So we find the block to start looking for, we pull in all the blocks we need then crop it to the amount required


        meta = getmetadata(path)
        dblocks = meta['blocks']

        whichblock = offset / BLOCKSIZE
        whereinblock = offset % BLOCKSIZE

        numblocks = size / BLOCKSIZE + 1
        
        #first grab the block names from dblocks
        blocks = dblocks[whichblock: whichblock+numblocks]
        #now actually get the blocks
        blocks = map(getblock, blocks)
        #now concat them into one huge chunk
        blocks = "".join(blocks)
        #now we have one huge chunk of thing which should be fine for what we need

        blocks = blocks[whereinblock:] #get rid of stuff before the start
        blocks = blocks[:size] #and only give the size requested for
        return blocks
    
    def readdir(self, path, fh):
        meta = getmetadata(path)
        return ['.', '..'] + [x.split("/")[-1] for x in meta['files']]
    
    def readlink(self, path):
        meta = getmetadata(path)
        if meta.get('islink', False):
            return meta['linksto']
    
    def removexattr(self, path, name):
        meta = getmetadata(path)
        if name in meta['xattr']:
            del meta['xattr'][name]
    
    def rename(self, old, new):
        self.files[new] = self.files.pop(old)
    
    def rmdir(self, path):
        self._rmfile(path)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        meta = getmetadata(path)
        meta['xattr'][name] = value
        setmetadata(path, meta)
    
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
        meta = getmetadata(target)
        print "TARGET META", meta
        srcmeta = getmetadata(source)
        print "SRC meta", srcmeta
        meta['mode'] = S_IFLNK | 0777
        meta['st_size'] = srcmeta['st_size']
        meta['st_nlink'] = 1
        meta['islink'] = True
        meta['linksto'] = source

        setmetadata(target, meta)

    def truncate(self, path, length, fh=None):
        #truncate path to length bytes
        meta = getmetadata(path)
        dblocks = meta['blocks']
        
        numblocks = length / BLOCKSIZE
        lastblocklen = length % BLOCKSIZE

        dblocks = dblocks[:numblocks]

        if len(dblocks) == 0:
            meta['blocks'] = dblocks
            setmetadata(path, meta)
            return

        lastblock = getblock(dblocks[-1])
        lastblock = lastblock[:lastblocklen]
        
        blockname = nameblock[block]
        putblock(blockname, lastblock)
        dblocks[-1] = blockname

        meta['blocks'] = dblocks
        setmetadata(path, meta)

    def unlink(self, path):
        meta = getmetadata(path)
        if 'files' in meta:
            return EISDIR
        
        self._rmfile(path) 

    def _rmfile(self, path):
        rmblock("METADATA=" + path)
        parent = getparent(path)
        parentmeta = getmetadata(parent)
        parentmeta['files'].remove(path)
        setmetadata(parent, parentmeta)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        meta = getmetadata(path)
        meta['st_atime'] = atime
        meta['st_mtime'] = mtime
        setmetadata(path, meta)

    def write(self, path, data, offset, fh):
#        print "Writing", data, path, offset, fh
        meta = getmetadata(path)
        dblocks = meta['blocks']

        whichblock = offset / BLOCKSIZE
        whereinblock = offset % BLOCKSIZE


        if whichblock >= len(dblocks):
            curblock = ""
        else:
            curblock = getblock(dblocks[whichblock])


        datalen = min(len(data), BLOCKSIZE) #only write one block at a time
        data = data[:datalen] #truncate data that can't be written

        curblock = curblock[:offset] + data + curblock[offset + datalen:]
        

        blockname = nameblock(curblock)

        putblock(blockname, curblock) #store the new block

        #now update the metadata
        if whichblock < len(dblocks):
            dblocks[whichblock] = blockname
        else:
            #new block, need to update the size
            sze = BLOCKSIZE * len(dblocks) #how many full blocks do we have?
            sze += len(curblock) #add in the length of the current block
            
            #New block, update size
            dblocks += [blockname]

            meta['st_size'] = sze


        meta['blocks'] = dblocks
        setmetadata(path, meta)

        return min(len(data), BLOCKSIZE)


if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(BasicFuse(), argv[1], foreground=True)
    #fuse = FUSE(BasicFuse(), argv[1])
