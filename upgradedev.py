#!/usr/bin/env python

import libxml2
import libxslt
import sys
import daemondev
import os
import gfx
import png

upgrade = """
<!-- Created by Yogesh Sharma-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
        <xsl:template match="/">
                <print embed_fonts="false">
                        <xsl:for-each select="print/control[ff!='null']">
                                <control>
                                        <text><xsl:value-of select="text"/></text>
                                        <x><xsl:value-of select="x+%(x)d"/></x>
                                        <y><xsl:value-of select="y+%(y)d"/></y>
                                        <fs><xsl:value-of select="round(fs*%(fs)d)"/></fs>
                                        <ff><xsl:value-of select="ff"/></ff>
                                        <biu><xsl:value-of select="biu"/></biu>
                                </control>
                        </xsl:for-each>
                </print>
        </xsl:template>
</xsl:stylesheet>
"""

def get_x_offset(rotation,p,page):
	"""Get the x offset for a page"""
	top,bottom,left,right = png.getMargins(p)
	return {
			0:left,
			90:top,
			180:right,
			270:bottom
		}[rotation]
	

def get_y_offset(rotation,p,page):
	"""Get the y offset for a page"""
	top,bottom,left,right = png.getMargins(p)
	return {
			0:top,
			90:right,
			180:bottom,
			270:left
		}[rotation]

def get_font_scale(rotation,p,page):
	"""Get the font scale factor
	which is currently 1 because
	we have matched the DPI of the
	pngs to the swfs"""
	return 1

def upgradeXML(filein,fileout,style):
	"""This function applys the XSL style 
	to filein and writes the result to fileout,
	if fileout is <value>None</value the result
	is printed to the screen"""
	if not os.path.exists(filein): return
	upgrade_style = libxml2.parseDoc(style)
	style_doc = libxslt.parseStylesheetDoc(upgrade_style)
	doc = libxml2.parseFile(filein)
	
	result = style_doc.applyStylesheet(doc, None)
	
	if fileout == None:
		print result
	else:
		style_doc.saveResultToFilename(fileout, result, 0)

	style_doc.freeStylesheet()
	doc.freeDoc()
	result.freeDoc()

def upgrade_page(base,doc,page,processed=None):
	"""Upgrades the "page" of the project
	if the user has rearranged the pages of the
	document the page getting passed in can
	correspond to different pages in the document"""
	
	#initilize processed pages
	if processed==None: processed=range(0,doc.pages)

	#check if page exists
	if not os.path.exists("%s/.old/%d"%(base,page)): 
		return {'folder':"%d" % processed.pop(0),'rotation':"0",'available':'0'}
	
	#assume real page number is normal page number
	real_page = page
	new_path = "%s/.old/%d/" % (base,page)

	#determine real page number
	if os.path.exists("%s/orig.txt" % new_path ):
		fh =  open("%s/orig.txt" % new_path)
		real_page = int( fh.read() )
		fh.close()

	rotation = 0
	#determine the page rotation
	if os.path.exists( "%s/rot.txt" % new_path ):
		rotation = int( open( "%s/rot.txt" % new_path ).read() )%360

	data={'x':0 , 'y':0, 'fs':1}
	png = "%s/img.png" % new_path
	if os.path.exists(png):
		data['x']=get_x_offset(rotation, png, real_page)
		data['y']=get_y_offset(rotation, png, real_page)
		data['fs']=get_font_scale(rotation, png, real_page)

	#transform the xml
	upgradeXML( "%s/values.txt" % new_path, "%s/%d/content.txt" % (base,real_page), upgrade % data )

	#return the page data
	processed.remove(real_page)
	return {'folder':"%d" % real_page,'rotation':"%d" % rotation,'available':'1'}
	
def move_pages(base,pages):
	"""moves the page folders to a .old directory
	so that the old data is saved for conversion"""
	#create the new directory
	newdir = "%s/.old" % base
	if os.path.isdir(newdir):
		os.chmod(newdir,daemondev.chmod_mask)
	elif os.path.isfile(newdir): 
		return false
	else:
		os.mkdir(newdir)
		os.chmod(newdir,daemondev.chmod_mask)
	
	for i in range(0,pages):
		if os.path.isdir("%s/%d"%(base,i)):
			os.rename( "%s/%d" % (base,i), "%s/.old/%d/" % (base,i) )

def main(fname):
	"""Upgrade a current project"""
	#get info about project
	fname = fname.strip()
	doc = gfx.open("pdf", fname)
	dir , junk = os.path.split(fname)

	#remove convert.ready this will be recreated
	#by the daemon
	if os.path.exists(dir + "/" + 'convert.ready'): 
		os.remove(dir + "/" + 'convert.ready')

	#move pages to .old directory
	move_pages(dir,doc.pages)

	#create new directories
	daemondev.make_dirs(dir,doc.pages)

	#upgrade the pages
	config = "<document>"
	for pagenr in range(0,doc.pages):
		page_data = upgrade_page(dir,doc,pagenr)
		config += """<page folder='%(folder)s' rotation='%(rotation)s' available='%(available)s' />""" %page_data
	config += "</document>"
	file = open("%s/config.xml"%dir,'w')
	file.write(config)
	file.close()

if __name__ == "__main__":
	filein = sys.argv[1]
	for i in sys.argv[2:]:
		filein += " " + i 
	file = open("/var/log/upgrade.log",'w')
	file.write("Upgrading: %s\n"%filein)
	file.close()
	main(filein)
