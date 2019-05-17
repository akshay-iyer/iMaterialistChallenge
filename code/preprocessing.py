import constants
import json
from multiprocessing.pool import ThreadPool
import os
import requests

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

	def pool_images(self,all_urls):
		try:
			results = ThreadPool(8).imap_unordered(self.fetch_url, all_urls)
			for path in results:
				print(path)
		except:
			print("URL Missing")


	def get_all_urls(self,path,data_type):
		if data_type == 'TRAINING':
			write_path = constants.TRAINING_IMAGES
		if data_type == 'VALIDATION':
			write_path = constants.VALIDATION_IMAGES
		if data_type == 'TESTING':
			write_path = constants.TESTING_IMAGES

		with open(path, "r") as read_file:
			data = json.load(read_file)
		all_urls = [(os.path.join(write_path,i['id']),i['url']) for i in data['images']]

		return all_urls


	def load_data(self,path,data_type):
		file_path = ""
		if data_type == 'TRAINING':
			file_path = os.path.join(constants.TRAINING_JSON)
		if data_type == 'VALIDATION':
			file_path = os.path.join(constants.VALIDATION_JSON)
		if data_type == 'TESTING':
			file_path = os.path.join(constants.TESTING_JSON)

		all_urls = self.get_all_urls(file_path,data_type)
		self.pool_images(all_urls)


		pass

if __name__ == '__main__':
	preproc = Preprocessor()

	if not os.path.exists(constants.TRAINING_IMAGES):
		os.makedirs(constants.TRAINING_IMAGES)
	#load training data
	self.load_data(constants.TRAINING_IMAGES,"TRAINING")

	if not os.path.exists(constants.VALIDATION_IMAGES):
		os.makedirs(constants.VALIDATION_IMAGES)
	#load validation data
	preproc.load_data(constants.VALIDATION_IMAGES,"VALIDATION")

	if not os.path.exists(constants.TESTING_IMAGES):
		os.makedirs(constants.TESTING_IMAGES)
	#load testing data
	preproc.load_data(constants.TESTING_IMAGES,"TESTING")