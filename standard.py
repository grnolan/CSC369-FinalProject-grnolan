# Standard Approach 

import json

def main():
    # language: number of repos 
    f = open('LANNGS1000rows.json')
    data = json.load(f)
    count_languages(data)
    
    # repo_name: file count and average # files per repo
    f = open('FILES1000rows.json')
    data = json.load(f)
    get_file_stats(data)
    
    # repo_name: commit count and average # commits per repo
    f = open('COMMITS100.json')
    data = json.load(f)
    get_commit_stats(data)
        
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
            
    for l in langs.keys():
        print(l + " : " + str(langs[l]))

        
def get_file_stats(data):
    repos = {}
    
    for row in data:
        if row['repo_name'] in repos.keys():
            repos[row['repo_name']] += 1
        else:
            repos[row['repo_name']] = 1
    
    for r in repos.keys():
        print(r + " : " + str(repos[r]))
        
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
        for repo in row['repo_name']:
            if repo in repos.keys():
                repos[repo] += 1
            else:
                repos[repo] = 1
    
    for r in repos.keys():
        print(r + " : " + str(repos[r]))
        
    total = 0
    n = 0
    avg = 0
    for r in repos.keys():
        total += repos[r]
        n += 1
    avg = total / n
    print("There are " + str(avg) + " commits on average to a repository")
    
    

if __name__ == "__main__":
    main()
