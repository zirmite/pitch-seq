import boto3
import requests as req
import pandas as pd
import time
import os


def read_game_log(filename):

  with open(filename, 'r') as game_log:
    df = pd.read_csv(filename, parse_dates=['Date'])

  return df


def generate_game_id(x):
  game_id = ''
  datestr = 'gid_' + x.Date.strftime('%Y-%m-%d').replace('-','_')
  teamstr = x.Away.lower() + 'mlb_' + x.Home.lower() + 'mlb_1'

  return datestr + '_' + teamstr


## http://gd2.mlb.com/components/game/mlb/year_2018/month_04/day_01/gid_2018_04_01_minmlb_balmlb_1/game.xml
def parse_game_log(df):

  df['year'] = df.Date.apply(lambda x: x.year)
  df['month'] = df.Date.apply(lambda x: x.month)
  df['day'] = df.Date.apply(lambda x: x.day)
  df['game_id'] = df.apply(generate_game_id, axis=1)

  base_url = 'http://gd2.mlb.com/components/game/mlb/year_{year}/month_{month:02d}/day_{day:02d}/{game_id}/'
  df['url'] = df.apply(
    lambda x: base_url.format(
      year=x.Date.year,
      month=x.Date.month,
      day=x.Date.day,
      game_id=x.game_id,
    ),
    axis=1
  )

  return df


def download_data(x, suffix='inning/inning_all.xml', sleep=2.5):
  r = req.get(x.url + suffix)
  time.sleep(sleep)

  if r.status_code == 200:
    return (x.game_id, suffix, r.content)
  else:
    return (x.game_id, suffix, None)


def download_all_data(x, sleep=2.5):
  (game_id, suffix, game_data) = download_data(x, suffix='game.xml', sleep=sleep)
  (game_id, isuffix, inning_data) = download_data(x, sleep=sleep)

  return (game_id, game_data, inning_data)


def upload_data(game_id, game_data, inning_data, s3):
  s3.Bucket('pitch-seq').put_object(Key='raw-data/'+game_id+'_game.xml', Body=game_data)
  s3.Bucket('pitch-seq').put_object(Key='raw-data/'+game_id+'_innings.xml', Body=inning_data)


def download_and_upload(x, s3, sleep=2.5):
  print(x.game_id)
  (game_id, game_data, inning_data) = download_all_data(x, sleep=sleep)
  upload_data(game_id, game_data, inning_data, s3)


if __name__ == "__main__":
    sess = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'blah'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'secret'),
        region_name='us-west-1'
    )
    s3 = sess.resource('s3')

    df = read_game_log("../data/GL2018_simplified.txt")
    df = parse_game_log(df)
    df.apply(lambda x: download_and_upload(x, s3), axis=1)
