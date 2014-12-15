#!/usr/bin/env python

import os, sys
import gamin
import gfx
import time
from multiprocessing import Process
from math import ceil
import stat
#import upgradev2

gfx.setparameter("zoom","96")

stack = "/var/pdffiller/pdffiller-stack/"
chmod_mask =  stat.S_IRUSR | stat.S_IXUSR |  stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP| stat.S_IXGRP | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH

processes = []

def make_dirs(base_dir, pages):
    base_dir += "/"
    for i in range(0, pages):
        newdir = base_dir + str(i)
        if not os.path.isdir(newdir):
            os.mkdir(newdir)
        os.chmod(newdir,chmod_mask)

def writeStatus(file, status):
    file.truncate(0)
    file.seek(0)
    file.write(status+"\n")
    file.flush()

def pdf2swf(fname, sname):
    try:
        dir, junk = os.path.split(fname)
        project_id = os.path.basename(dir)
        
        status = file(dir + "/" + 'convert.status', 'w+')
        try:
            
            doc = gfx.open("pdf", fname)
            
            make_dirs(dir, doc.pages)
            writeStatus(status, "Progress: %d/%d" % (0, doc.pages))
            
            swf = gfx.SWF()
            swf.setparameter("flashversion", "9"); 
            for pagenr in xrange(1,doc.pages+1):
                page = doc.getPage(pagenr)
                swf.startpage(page.width, page.height)
                page.render(swf)
                swf.endpage()
                writeStatus(status, "Progress: %d/%d" % (pagenr, doc.pages))
            swf.save(dir + "/" + "doc_temp.swf")
            os.rename(dir + "/" + "doc_temp.swf",dir + "/" + "doc.swf") 
            
            open(dir + "/" + 'convert.ready', 'w').close() # FIXME: for backward support
            writeStatus(status, "Finished")
            os.system('chown -R www-data:www-data ' + dir)
            os.system("php /var/www/html/production/ajax/cs.php "+project_id)
        except:
            writeStatus(status, "Error: " + str(sys.exc_info()[0]) + ":" + str(sys.exc_info()[1]))
    finally:
        os.remove(sname)

def clean_processes():
    for i in processes:
        if not i.is_alive(): 
            i.join()
            processes.remove(i)
            print "Thread closed"

def callback(path, event):
    if event not in (gamin.GAMChanged, gamin.GAMExists): return
    try:
        if os.path.isdir(path): return
        file_pointer = open("%s%s" % (stack,path))
    except IOError:
        print 'IOError skipping'
    else:
        data = file_pointer.read()
        data = data.strip()
        print data
        file_pointer.close()
        
	p = Process(target=pdf2swf,args=(data,"%s%s" % (stack,path)))
        p.start()
        processes.append(p)
        print "File " , path  , " " , event
        

if __name__=="__main__":
    try:
        mon = gamin.WatchMonitor()
        mon.watch_directory(stack, callback)
        time.sleep(1)
        while 1:
            mon.event_pending()    
            mon.handle_one_event()  ##blocking call
            time.sleep(.1)
            clean_processes()
            print "Number of active threads: ",  len(processes)
    except KeyboardInterrupt:
        print "goodbye"
