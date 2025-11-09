import re
from nltk.corpus import stopwords
from nltk import download
from nltk.tokenize import word_tokenize
import holidays

download("stopwords")
download("punkt_tab")
pt_stopwords = stopwords.words("portuguese")


def pre_processing(content: str):
    content = re.sub(r"[^\w\s]", "", content).lower()
    return " ".join(
        [
            w
            for w in word_tokenize(content, language="portuguese")
            if w not in pt_stopwords
        ]
    )


br_holidays = holidays.BR()


def is_weekend(x):
    day = x.weekday()
    return day >= 5 or x in br_holidays
