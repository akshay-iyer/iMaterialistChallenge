import constants
import json
from multiprocessing.pool import ThreadPool
import os
import requests

import sys,multiprocessing, urllib3, csv
from PIL import Image
from io import BytesIO
from tqdm  import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Preprocessor:
	def __init__(self):
		self
		pass

	def fetch_url(self,entry):
		path, uri = entry
		#path = os.path.join("data/training_images",path_id)
		if not os.path.exists(path):
			r = requests.get(uri, stream=True)
			if r.status_code == 200:
				with open(path, 'wb') as f:
					for chunk in r:
						f.write(chunk)
		return path

	def download_image(self,key_url):
		#out_dir = sys.argv[2]
		(filename, url) = key_url
		#filename = os.path.join(out_dir, '%s.jpg' % key)
		#filename = key

		if os.path.exists(filename):
			print('Image %s already exists. Skipping download.' % filename)
			return

		try:
			global client
			response = client.request('GET', url)#, timeout=30)
			image_data = response.data
		except:
			print('Warning: Could not download image %s from %s' % (filename, url))
			return

		try:
			pil_image = Image.open(BytesIO(image_data))
		except:
			print('Warning: Failed to parse image %s %s' % (filename,url))
			return

		try:
			pil_image_rgb = pil_image.convert('RGB')
		except:
			print('Warning: Failed to convert image %s to RGB' % filename)
			return

		try:
			pil_image_rgb.save(filename, format='JPEG', quality=90)
		except:
			print('Warning: Failed to save image %s' % filename)
			return


	def pool_images(self,key_url_list):
		try:
			results = ThreadPool(8).imap_unordered(self.fetch_url, key_url_list)
			for path in results:
				print(path)
		except:
			print("URL Missing")


	def download_images(self,key_url_list):
		pool = multiprocessing.Pool(processes=12)

		with tqdm(total=len(key_url_list)) as bar:
			for _ in pool.imap_unordered(self.download_image, key_url_list):
			  bar.update(1)



	def get_key_url_list(self,path,data_type):
		if data_type == 'TRAINING':
			write_path = constants.TRAINING_IMAGES
		if data_type == 'VALIDATION':
			write_path = constants.VALIDATION_IMAGES
		if data_type == 'TESTING':
			write_path = constants.TESTING_IMAGES

		with open(path, "r") as read_file:
			data = json.load(read_file)
		key_url_list = [(os.path.join(write_path,i['id']),i['url']) for i in data['images']]

		return key_url_list


	def load_data(self,path,data_type):
		file_path = ""
		if data_type == 'TRAINING':
			file_path = os.path.join(constants.TRAINING_JSON)
		if data_type == 'VALIDATION':
			file_path = os.path.join(constants.VALIDATION_JSON)
		if data_type == 'TESTING':
			file_path = os.path.join(constants.TESTING_JSON)

		key_url_list = self.get_key_url_list(file_path,data_type)
		self.download_images(key_url_list)


		pass

if __name__ == '__main__':
	preproc = Preprocessor()

	client = urllib3.PoolManager(500)

	if not os.path.exists(constants.TRAINING_IMAGES):
		os.makedirs(constants.TRAINING_IMAGES)
	#load training data
	preproc.load_data(constants.TRAINING_IMAGES,"TRAINING")

	if not os.path.exists(constants.VALIDATION_IMAGES):
		os.makedirs(constants.VALIDATION_IMAGES)
	#load validation data
	preproc.load_data(constants.VALIDATION_IMAGES,"VALIDATION")

	if not os.path.exists(constants.TESTING_IMAGES):
		os.makedirs(constants.TESTING_IMAGES)
	#load testing data
	preproc.load_data(constants.TESTING_IMAGES,"TESTING")