from PythonMagick import *
import os

def getMargins(png):
	"""returns the margins of a cropped png file
	the margins are returned as a tuple of the form
	(top,bottom,left,right).  If the file does not exist
	this function returns None"""
	def exists(file):
		return os.path.exists(file) and os.path.isfile(file)

	def getPageWidth(image):
		return image.page().width()

	def getPageHeight(image):
		return image.page().height()

	def getImageHeight(image):
		return image.size().height()
	
	def getImageWidth(image):
		return image.size().width()

	def getLeftMargin(image):
		return image.page().xOff()

	def getTopMargin(image):
		return image.page().yOff()

	if exists(png):
		i = Image(png)
		pheight = getPageHeight(i) 
		pwidth = getPageWidth(i)
		iheight = getImageHeight(i)
		iwidth = getImageWidth(i)

		top = getTopMargin(i)
		bottom = pheight-iheight-top
		left = getLeftMargin(i)
		right = pwidth-iwidth-left

		return (top,bottom,left,right)

	else:
		return None

