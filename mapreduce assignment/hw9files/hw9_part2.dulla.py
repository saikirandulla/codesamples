###
###
# Author Info: 
#     This code is modified from code originally written by Jim Blomo and Derek Kuo
##/

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


class UserSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    def mapper1_extract_user_business(self,_,record):
        """Taken a record, yield <user_id, business_id>"""
        yield [record['user_id'], record['business_id']]
       

    def reducer1_compile_businesses_under_user(self,user_id,business_ids):
        #print "reducer 1"
        #print business_ids
        #### TODO_1: compile businesses as a list of array under given user_id,after remove duplicate business, 
        business_id_list = list(set(business_ids))
#        print business_id_list
        yield [user_id, business_id_list]
        
        
    
    def mapper2_collect_businesses_under_user(self, user_id, business_ids):
        #print user_id, business_ids
        #### TODO_2: collect all <user_id, business_ids> pair, map into the same Keyword LIST, yield <'LIST',[user_id, [business_ids]]>
        ##/        
        yield ['LIST',[user_id, business_ids]]
        #print ['LIST',[user_id,[business_ids]]]

    def reducer2_calculate_similarity(self,stat,user_business_ids):
        def Jaccard_similarity(business_list1, business_list2):        
            
            len_list1 = len(business_list1)
            len_list2 = len(business_list2)
            intersection = len(set(business_list1)&set(business_list2))

            jaccard_num = intersection/float(len_list1 + len_list2 - intersection)
            return jaccard_num

        k = list(user_business_ids)
        count = 0
        for i in k:
            #print i[1]            
            for j in k[count + 1:]:
                #print j[1]
                score = Jaccard_similarity(i[1],j[1])
                if score >= 0.5:
                    yield [[i[0],j[0]],score]
                    
            count +=1
        #print count
        ### TODO_4: Calulate Jaccard, output the pair users that have similarity over 0.5, yield <[user1,user2], similarity>
        ##  
    def steps(self):
        return [
            self.mr(mapper = self.mapper1_extract_user_business, reducer = self.reducer1_compile_businesses_under_user),
            self.mr(mapper = self.mapper2_collect_businesses_under_user, reducer = self.reducer2_calculate_similarity)
        ]


if __name__ == '__main__':
    UserSimilarity.run()