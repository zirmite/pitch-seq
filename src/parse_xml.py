import boto3
import xml.etree.ElementTree as ET


class Game:
  def __init__(self, xmlfile):
    self.game = ET.parse(xmlfile).getroot()
    self.innings = [Inning(inning) for inning in game.findall('inning')]


class Inning:
  def __init__(self, xmlinning):
    self.num = xmlinning.get('num')
    self.top = HalfInning(xmlinning, 'top')
    self.bottom = HalfInning(xmlinning, 'bottom')


class HalfInning:
  def __init__(self, xmlinning, type):
    self.type = type
    self.xml = xmlinning.findall(type)[0]
    self.atbats = [AtBat(atbat) for atbat in self.xml.findall('atbat')]


class AtBat:
  def __init__(self, xmlatbat):
    self.

def parse_xml(inning_file):

  game = ET.parse(inning_file).getroot()
