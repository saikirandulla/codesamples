from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import re

class Similarwords(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper1_extract_reviews_for_each_user(self, _, record):
        yield record['user_id'], record['text']

    def reducer1_generates_sets_of_common_words_for_each_user(self, user_id, reviews):
        words_per_review = []

        for review in reviews:
            words_per_review.append(set(re.findall(r"[\w']+", review.lower())))

        common_words = list(set.intersection(*words_per_review))
        yield (user_id, common_words)

    def mapper2_collect_words_under_user(self, user_id, common_words):
        yield ('LIST', [user_id,common_words])
        ##
        # TODO_2: collect all <user_id, business_ids> pair, map into the same Keyword LIST, yield <'LIST',[user_id, [business_ids]]>
         
    def reducer2_calculate_similarity(self,stat,user_common_words):

        def Jaccard_similarity(word_list1, word_list2):
            jaccard_intersection = list(set(word_list1).intersection(word_list2))
            #print jaccard_intersection
            jaccard_operation = float(float(len(jaccard_intersection))/(float(0.000001)+float((len(word_list1)) + float(len(word_list2)) - float(len(jaccard_intersection)))))
            return jaccard_operation

        whole_users=[x for x in user_common_words]

        
        for h in range(0,len(whole_users)-1):
           
            for i in range(0,len(whole_users)):

                if h != i:
                    wl1 = whole_users[h][1]
                    
                    wl2 = whole_users[i][1]
                    
                    jaccard_result = Jaccard_similarity(wl1,wl2)

                    if jaccard_result >= 0.5:
                        print whole_users[i][0],whole_users[h][0], jaccard_result
                        


    def steps(self):
        return [
            self.mr(mapper=self.mapper1_extract_reviews_for_each_user, reducer=self.reducer1_generates_sets_of_common_words_for_each_user),
            self.mr(mapper=self.mapper2_collect_words_under_user, reducer=self.reducer2_calculate_similarity),
            ]

if __name__ == '__main__':
    Similarwords.run()