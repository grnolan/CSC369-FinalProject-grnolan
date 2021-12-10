# Standard Approach 

import json
import time

def main():
    
    print("--- STANDARD LANGUAGES ---")
    # language: number of repos 
    f = open('langs.json')
    data = json.load(f)
    start_time = time.time()
    count_languages(data)
    print("--- %s seconds ---" % (time.time() - start_time))
    

    print("--- STANDARD FILES ---")
    # repo_name: file count and average # files per repo
    f = open('files.json')
    data = json.load(f)
    start_time = time.time()
    get_file_stats(data)
    print("--- %s seconds ---" % (time.time() - start_time))
    
    
    print("--- STANDARD COMMITS ---")
    # repo_name: commit count and average # commits per repo
    f = open('commits.json')
    data = json.load(f)
    start_time = time.time()
    get_commit_stats(data)
    print("--- %s seconds ---" % (time.time() - start_time))
    
        
def count_languages(data):
    langs = {}
    langs["none"]  = 0

    for row in data:
        if len(row['language']) < 1:
            langs["none"] += 1
        else:
            for i in range(len(row['language'])):
                if row['language'][i]['name'] in langs.keys():
                    langs[row['language'][i]['name']] += 1
                else:
                    langs[row['language'][i]['name']] = 1
                    
    #print all results     
    lesses = 0
    tot = 0
    for l in langs.keys():
        tot += langs[l]
        if langs[l] > 1000:
            print(l + ":" + str(langs[l]))
        else:
            lesses += 1
    
    most = max(langs, key=langs.get)
    print(most)
    print(langs[most])
    
    
        
def get_file_stats(data):
    repos = {}
    
    for row in data:
        if row['repo_name'] in repos.keys():
            repos[row['repo_name']] += 1
        else:
            repos[row['repo_name']] = 1
    
    #print all results
    #for r in repos.keys():
    #    print(r + " : " + str(repos[r]))
        
    #find average
    total = 0
    n = 0
    avg = 0
    for r in repos.keys():
        total += repos[r]
        n += 1
    avg = total / n
    print("There are " + str(avg) + " files on average in a repository")
    

def get_commit_stats(data):
    repos = {}
    
    for row in data:
        repo = row['repo_name']
        if repo in repos.keys():
            repos[repo] += 1
        else:
            repos[repo] = 1
    
    for r in repos.keys():
        print(r + " : " + str(repos[r]))

    #find average
    total = 0
    n = 0
    avg = 0
    for r in repos.keys():
        print(r)
        print(repos[r])
        total += repos[r]
        n += 1
    avg = total / n
    print("There are " + str(avg) + " commits on average to a repository")
    
    

if __name__ == "__main__":
    main()
