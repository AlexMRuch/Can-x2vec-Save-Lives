import sys
import os
import random
from collections import Counter
from time import time

class MetaPathGenerator:
	def __init__(self):
		self.id_author = dict()
		self.id_conf = dict()
		self.id_paper = dict()
		#self.author_coauthorlist = dict()
		self.conf_authorlist = dict()
		self.author_conflist = dict()
		self.paper_author = dict()
		self.author_paper = dict()
		self.conf_paper = dict()
		self.paper_conf = dict()

	def read_data(self, dirpath):
		with open(dirpath + "/id_author.txt") as adictfile:
			for line in adictfile:
				toks = line.strip().split("\t")
				if len(toks) == 2:
					self.id_author[toks[0]] = toks[1].replace(" ", "")

		print("#authors", len(self.id_author))

		with open(dirpath + "/id_paper.txt") as pdictfile:
			for line in pdictfile:
				toks = line.strip().split("\t")
				if len(toks) == 2:
					newpaper = toks[1].replace(" ", "")
					self.id_paper[toks[0]] = newpaper

		print("#paper", len(self.id_paper))

		with open(dirpath + "/id_conf.txt") as cdictfile:
			for line in cdictfile:
				toks = line.strip().split("\t")
				if len(toks) == 2:
					newconf = toks[1].replace(" ", "")
					self.id_conf[toks[0]] = newconf

		print("#conf", len(self.id_conf))

		with open(dirpath + "/paper_author.txt") as pafile:
			for line in pafile:
				toks = line.strip().split("\t")
				if len(toks) == 2:
					p, a = toks[0], toks[1]
					if p not in self.paper_author:
						self.paper_author[p] = []
					self.paper_author[p].append(a)
					if a not in self.author_paper:
						self.author_paper[a] = []
					self.author_paper[a].append(p)

		with open(dirpath + "/paper_conf.txt") as pcfile:
			for line in pcfile:
				toks = line.strip().split("\t")
				if len(toks) == 2:
					p, c = toks[0], toks[1]
					self.paper_conf[p] = c
					if c not in self.conf_paper:
						self.conf_paper[c] = []
					self.conf_paper[c].append(p)

		sumpapersconf, sumauthorsconf = 0, 0
		for conf in self.conf_paper:
			self.conf_authorlist[conf] = []
			papers = self.conf_paper[conf]
			sumpapersconf += len(papers)
			for paper in papers:
				if paper in self.paper_author:
					authors = self.paper_author[paper]
					sumauthorsconf += len(authors)
					for author in authors:
						self.conf_authorlist[conf].append(author)
						if author not in self.author_conflist:
							self.author_conflist[author] = []
						self.author_conflist[author].append(conf)
						# author_coauthorlist overflows RAM and is not used
						#if author not in self.author_coauthorlist:
							#self.author_coauthorlist[author] = []
						#self.author_coauthorlist[author].extend(authors)

		print("author-conf list done")

		print("#confs  ", len(self.conf_paper))
		print("#papers ", sumpapersconf,  "#papers per conf ", sumpapersconf / len(self.conf_paper))
		print("#authors", sumauthorsconf, "#authors per conf", sumauthorsconf / len(self.conf_paper))


	def generate_random_cac(self, outfilename, numwalks, walklength):
		outfile = open(outfilename, 'w')
		node_counter_i = 0
		node_counter_n = len(self.conf_authorlist)
		time_start = time()
		for conf in self.conf_authorlist:
			conf0 = conf
			for j in range(0, numwalks): #wnum walks
				outline = self.id_conf[conf0]
				for i in range(0, walklength):
					authors = self.conf_authorlist[conf]
					numa = len(authors)
					authorid = random.randrange(numa)
					author = authors[authorid]
					outline += " " + self.id_author[author]
					confs = self.author_conflist[author]
					numc = len(confs)
					confid = random.randrange(numc)
					conf = confs[confid]
					outline += " " + self.id_conf[conf]
				outfile.write(outline + "\n")
			node_counter_i += 1
			mean_time = (time()-time_start) / node_counter_i
			print("Processed node {}/{}:".format(node_counter_i,node_counter_n))
			print("  Mean time:", mean_time)
			print("  Hrs. left:", (node_counter_n-node_counter_i)/(60/mean_time)/60)
		outfile.close()
		print("\n************************************************************\n")
		print("  **** Done writing file to {} ****  ".format(outfilename))
		print("\n************************************************************\n")


	def generate_random_csasc(self, outfilename, numwalks, walklength):
		outfile = open(outfilename, 'w')
		node_counter_i = 0
		node_counter_n = len(self.conf_authorlist)
		time_start = time()
		for conf in self.conf_authorlist:
			conf0 = conf
			for j in range(0, numwalks): #wnum walks
				outline = self.id_conf[conf0]
				for i in range(0, walklength):
					papers = self.conf_paper[conf]
					nump = len(papers)
					paperid = random.randrange(nump)
					paper = papers[paperid]
					outline += " " + self.id_paper[paper]
					authors = self.paper_author[paper]
					numa = len(authors)
					authorid = random.randrange(numa)
					author = authors[authorid]
					outline += " " + self.id_author[author]
					papers = self.author_paper[author]
					nump = len(papers)
					paperid = random.randrange(nump)
					paper = papers[paperid]
					outline += " " + self.id_paper[paper]
					confs = self.paper_conf[paper]
					numc = len(confs)
					confid = random.randrange(numc)
					conf = confs[confid]
					outline += " " + self.id_conf[conf]
				outfile.write(outline + "\n")
			node_counter_i += 1
			mean_time = (time()-time_start) / node_counter_i
			print("Processed node {}/{}:".format(node_counter_i,node_counter_n))
			print("  Mean time:", mean_time)
			print("  Hrs. left:", (node_counter_n-node_counter_i)/(60/mean_time)/60)
		outfile.close()
		print("\n************************************************************\n")
		print("  **** Done writing file to {} ****  ".format(outfilename))
		print("\n************************************************************\n")


#python py4genMetaPaths.py 1000 100 aca net_aminer output.aminer.w1000.l100.txt
#python py4genMetaPaths.py 1000 100 aca net_dbis   output.dbis.w1000.l100.txt

dirpath = "net_aminer"
# OR
dirpath = "net_dbis"

numwalks = int(sys.argv[1])
walklength = int(sys.argv[2])
mpg_metapath = str(sys.argv[3])

dirpath = sys.argv[4]
outfilename = sys.argv[5]

def main():
	mpg = MetaPathGenerator()
	mpg.read_data(dirpath)
	if mpg_metapath == "cac":
		print("Running sampler over cac metapath...\n")
		mpg.generate_random_cac(outfilename, numwalks, walklength)
	elif mpg_metapath == "csasc":
		print("Running sampler over csasc metapath...\n")
		mpg.generate_random_csasc(outfilename, numwalks, walklength)

if __name__ == "__main__":
	main()
