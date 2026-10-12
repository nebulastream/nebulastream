from collections import Counter
import heapq


# U14.	Frequentterms: Returns a space separated text containing the most N% frequent tokens 

def frequentterms(self,input:str,N:int)->str:
    if input:
        try:
            words_list = input.split()
            word_counts = Counter(word.lower() for word in words_list)
            frequent_terms = heapq.nlargest(N, word_counts, key=word_counts.get)
            return ' '.join([word for word in frequent_terms])
        except:
            return ''
    else:
        return None