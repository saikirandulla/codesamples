###
# Author Info: 
#     This code is modified from code originally written by Jim Blomo and Derek Kuo
##/
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import re

WORD_RE = re.compile(r"[\w']+")

class Similarwords(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1_extract_words_user(self, _, record):
        yield [record["user_id"],record["text"]]

    def reducer1_user_reviews_list(self,user_id, reviews):
        user_review_list = []
        for review in reviews:
            user_review_list.append(set(re.findall(r"\w']+",review.lower())))
        commonly_used_words = list(set.intersection(*user_review_list))
        yield(user_id,user_review_list)

    def mapper2_required_words(self, user_id, commonly_used_words):
        yield('LIST',[user_id,commonly_used_words])
   
    def reducer2_similarity(self,stat,user_common_words):

        def Jaccard_similarity(words_list1, words_list2):
            
            jaccard_intersection = list(set(words_list1).intersection(words_list2))
#            print jaccard_intersection
            jaccard_num = float(float(len(jaccard_intersection))/(float(0.000001)+float((len(words_list1)) + float(len(words_list2)) - float(len(jaccard_intersection)))))
            return jaccard_num

        whole_users=[x for x in user_common_words]
#        print whole_users
        
        for i in range(0,len(whole_users)-1):
            for j in range(0,len(whole_users)):
                if i != j:
                    wl1 = whole_users[i][1]
#                    print wl1
                    wl2 = whole_users[j][1]
#                    print wl2
                    jaccard_result = Jaccard_similarity(wl1,wl2)
                    if jaccard_result >= 0.5:
                        print whole_users[j][0],whole_users[i][0], jaccard_result


    def steps(self):
        return [
            self.mr(mapper=self.mapper1_extract_words_user, reducer=self.reducer1_user_reviews_list),
            self.mr(mapper=self.mapper2_required_words, reducer=self.reducer2_similarity),
            ]

if __name__ == '__main__':
    Similarwords.run()