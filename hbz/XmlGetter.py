#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# generating xml from EDB data
#
# 2018-01-xx
# david.klober@hbz-nrw.de

from Config import Config
import MySQLdb as mdb
import dicttoxml
import re
import string
import sys
from optparse import OptionParser


class Institution:
	identifier  = ''
	name        = ''
	sortname    = ''
	street      = ''
	zip         = ''
	city        = ''
	pobox       = ''
	country     = ''
	county      = ''
	funder_type = ''
	library_network = ''
	library_type = ''
	fte_students = ''
	fte_staff   = ''
	url         = ''
	billing_address = ''
	private_property = ''
	person      = ''


class XmlGetter:
	
	fileValid 		 = ''
	fileValidName 	 = 'result.Institutions.xml'
	fileNonValid 	 = ''
	fileNonValidName = 'result.Institutions.nonValid.xml'

	query_tu_timestamp = ''
	query_as_timestamp = ''

	institution         = []
	nonValidInstitution = []


	def do(self):

		self.fileValid    = open(self.fileValidName, 'w')
		self.fileNonValid = open(self.fileNonValidName, 'w')

		parser = OptionParser()
		parser.add_option("-t", "--tu", dest="opt_tu", help="only entries with edb.Teilnehmer_Update_Timestamp > t", metavar="2017-05-28")
		parser.add_option("-a", "--as", dest="opt_as", help="only entries with edb.aktualisierungStammdaten > a", metavar="2017-05-28")

		(options, args) = parser.parse_args()

		print "- options: %s, args: %s" % (options, args)

		if options.opt_tu:
			self.query_tu_timestamp = options.opt_tu
			print "- query items only with edb.Teilnehmer_Update_Timestamp > %s" % options.opt_tu
		if options.opt_as:
			self.query_as_timestamp = options.opt_as
			print "- query items only with edb.aktualisierungStammdaten > %s" % options.opt_as

		self.process()
		self.printStats()

		funcRenameItems = lambda x: x
		
		xml1 = dicttoxml.dicttoxml(self.institution, custom_root='institution', attr_type=False, item_func=funcRenameItems)
		self.fileValid.write(xml1)

		xml2 = dicttoxml.dicttoxml(self.nonValidInstitution, custom_root='nonValidInstitution', attr_type=False, item_func=funcRenameItems)
		self.fileNonValid.write(xml2)


	def process(self):

		try:
			config = Config()
			con = mdb.connect(config.dbhost, config.dbuser, config.dbpass, config.dbname, charset='utf8')
			cur = con.cursor(mdb.cursors.DictCursor)

			query = "SELECT t.*, l.Bundesland, l.Staat FROM Teilnehmer t join Land l on t.Land_ID = l.id"
			query1 = ''
			query2 = ''

			if self.query_tu_timestamp:
				query1 = "t.Teilnehmer_Update_Timestamp > '" + self.query_tu_timestamp + "'"

			if self.query_as_timestamp:
				query2 = "t.aktualisierungStammdaten > '" + self.query_as_timestamp + "'"

			if query1 or query2:
				query += " WHERE "
				if query1 and query2:
					query += query1 + " AND " + query2
				else:
					query += query1
					query += query2

			print "- %s" % query

			cur.execute(query)

			for instRow in cur.fetchall():

				inst = Institution()
				inst.source           = 'edb des hbz'

				inst.tu_timestamp = self.norm("%s" % instRow['Teilnehmer_Update_Timestamp'])
				inst.as_timestamp = self.norm("%s" % instRow['aktualisierungStammdaten'])

				inst.name             = self.norm(instRow['Name'])
				inst.sortname         = self.norm(instRow['Sortiername'])
				inst.street           = self.norm(instRow['Strasse'])
				inst.zip              = self.norm(instRow['PLZ'])
				inst.city             = self.norm(instRow['Ort'])
				inst.country          = self.norm(instRow['Staat'])

				if instRow['Art']:
					if instRow['Art'] == 'UB':
						inst.library_type = 'Universität'
					if instRow['Art'] == 'FHB':
						instRow['Art'] == 'Fachhochschule'
					if instRow['Art'] == 'OEB':
						inst.library_type = 'Öffentliche Bibliothek '
					if instRow['Art'] == 'KuMuHoB':
						inst.library_type = 'Kunst- und Musikhochschule'
					if instRow['Art'] == 'SpeziB':
						inst.library_type = 'Wissenschaftliche Spezialbibliothek'
					if instRow['Art'] == 'InstB':
						inst.library_type = 'Institutsbibliothek'
					if instRow['Art'] == 'Sonstige':
						inst.library_type = 'Sonstige'

				if instRow['Verbundszugehoerigkeit']:
					inst.library_network = self.norm(instRow['Verbundszugehoerigkeit'])

				if instRow['privat']:
					inst.funder_type = self.norm("%s" % instRow['privat'])

				if instRow['Postfach']:
					inst.pobox = self.norm(instRow['Postfach'])

				if instRow['Bundesland']:
					inst.county = self.norm(instRow['Bundesland'])

				if instRow['URL']:
					inst.url = self.norm(instRow['URL'])

				inst.identifier = {}

				if instRow['Sigel']:
					inst.identifier.update({'edb': self.norm(instRow['Sigel'])})

				if instRow['ISIL']:
					inst.identifier.update({'isil': self.norm(instRow['ISIL'])})

				if instRow['EZB_ID']:
					inst.identifier.update({'ezb': self.norm(instRow['EZB_ID'])})

				if instRow['WIB_ID']:
					inst.identifier.update({'wib': self.norm(instRow['WIB_ID'])})

				if instRow['RNAdresse']:
					rna = instRow['RNAdresse'].split("<br>")
					inst.billing_address = {}

					if len(rna) > 0:
						inst.billing_address.update({'name': self.norm(rna[0])})
					if len(rna) > 1:
						inst.billing_address.update({'street': self.norm(rna[1])})
					if len(rna) > 2:
						zipCity = rna[2].split(" ")

						if len(zipCity) > 0:
							inst.billing_address.update({'zip': self.norm(zipCity[0])})
						if len(zipCity) > 1:
							inst.billing_address.update({'city': self.norm(zipCity[1])})

				if instRow['Sigel']:
					cur.execute("SELECT * FROM Ansprechpartner WHERE Teilnehmer_Sigel = '" + instRow['Sigel'] + "'")
					ansprechpartner = cur.fetchall()

					inst.person = []

					if len(ansprechpartner) > 0:
						for ap in ansprechpartner:

							cp = {}
							cp.update({'first_name': self.norm("%s" % (ap['Vorname']))})
							cp.update({'last_name': self.norm("%s" % (ap['Name']))})

							if ap['sex']:
								if ap['sex'] == 'm':
									cp.update({'gender': 'Männlich'})
								elif ap['sex'] == 'w':
									cp.update({'gender': 'Weiblich'})

							if ap['email']:
								cp.update({'email': self.norm(ap['email'])})

							if ap['Telefon']:
								cp.update({'telephone': self.norm(ap['Telefon'])})

							if ap['FAX']:
								cp.update({'fax': self.norm(ap['FAX'])})

							if ap['Funktion']:
								cp.update({'function': self.norm(ap['Funktion'])})

							inst.person.append(cp)

					cur.execute("SELECT * FROM FTE_zahlen WHERE fte_sigel = '" + instRow['Sigel'] + "' ORDER BY fte_year DESC LIMIT 1")
					ftes = cur.fetchall()

					if len(ftes) == 1:
						for fte in ftes:
							if fte['FTE_Studierende']:
								inst.fte_students = self.norm("%s" % fte['FTE_Studierende'])

							if fte['FTE_Lehrpersonal']:
								inst.fte_staff = self.norm("%s" % fte['FTE_Lehrpersonal'])


				inst.private_property = {}

				if instRow['FragebogenEbene']:
					if instRow['FragebogenEbene'] == 'Stammkunden':
						inst.private_property.update({'fragebogenteilnehmer': 'Stammkunde'})
					if instRow['FragebogenEbene'] == 'Power-User':
						inst.private_property.update({'fragebogenteilnehmer': 'Power-User'})
					if instRow['FragebogenEbene'] == 'kein Teilnehmer':
						inst.private_property.update({'fragebogenteilnehmer': 'kein Teilnehmer'})

				if instRow['Promotionsrecht']:
					if instRow['Promotionsrecht'] in ('mit', 'ja'):
						inst.private_property.update({'promotionsrecht': 'Ja'})
					if instRow['Promotionsrecht'] in ('ohne', 'nein'):
						inst.private_property.update({'promotionsrecht': 'Nein'})

				if self.check(inst):
					self.institution.append(inst.__dict__)
				else:
					self.nonValidInstitution.append(inst.__dict__)

		except mdb.Error, e:

			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)

		finally:

			if con:
				con.close()


	def printStats(self):

		all = len(self.institution) + len(self.nonValidInstitution)

		print '\n'
		print 'Datei:  %s' % self.fileValidName
		print 'Gültig: %d von %d \n' % (len(self.institution), all)

		#for inst in self.institution:
		#	print inst.__dict__

		print 'Datei:  %s' % self.fileNonValidName
		print 'Ungültig: %d von %d \n' %(len(self.nonValidInstitution), all)

		#for inst in self.nonValidInstitution:
		#	print inst.__dict__


	def norm(self, str):

		str = re.sub('\s*<br\s?/?>\s*', ' ', str)
		return str.encode('UTF-8')


	def check(self, institution):

		return True



xg = XmlGetter()
xg.do()
