FROM python:3.9

RUN pip3 install nltk kafka-python textblob tweepy pycountry langdetect scikit-learn pandas
ADD main.py .

CMD ["python3", "./main.py"]
