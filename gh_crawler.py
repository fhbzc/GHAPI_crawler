import stscraper as scraper
import progressbar
import json
from collections import defaultdict
import requests
import time

def gh_api_validation(api_token_string):
    api_limit_list = list(scraper.get_limits(api_token_string))

    for api_info in api_limit_list:
      if api_info['core_limit'] != 5000:
        print("API has error")
        print("api_info",api_info)
        return 1 # has error

    return 0
valid_login_error_constant_check = 'Could not resolve to a User with the login of'
valid_repository_error_constant_check = 'Could not resolve to a Repository with the name'

class GHAPI_Crawler():

    def __init__(self, apitoken_list):
        github_api_token_string = ','.join(apitoken_list)
        assert gh_api_validation(github_api_token_string) == 0 # 0 for no error

        self.gh_api = scraper.GitHubAPIv4(github_api_token_string)



    def get_repo2issue2comments(self,
                          repo_slug_list,
                          saved_directory,
                          save_frequency = 1000,
                          repo2issue2comments = {}):

        p = progressbar.ProgressBar()
        for repo_slug_index in p(range(len(repo_slug_list))):
          repo_slug = repo_slug_list[repo_slug_index]
          if repo_slug in repo2issue2comments:
            continue
          owner, repo_name = repo_slug.split('/')
          # issue_number2title = {}
          # issue_number2body = {}
          issue_number2comment = {}
          try:
            issues = self.gh_api.v4("""
            query ($owner: String!, $name:String!,$cursor: String) {
              repository(owner: $owner, name: $name){
                issues(first:100, after: $cursor){
                  nodes{
                    number
                    
                    body
                  }
                  pageInfo{
                    hasNextPage
                    endCursor
                  }
                }
              }
            }"""
            ,owner = owner, name = repo_name)
            for issue in issues:
              issue_number = int(issue['number'])
              issue_number2comment.setdefault(issue_number, [])
              issue_number2comment[issue_number].append(issue['body'])

            for issue_number in issue_number2comment:
              issue_comments = self.gh_api.v4("""
              query ($owner: String!, $name:String!,$number: Int!, $cursor: String) {
                repository(owner: $owner, name: $name){
                  issue(number:$number){
                    comments(first:100, after: $cursor){
                      nodes{
                        bodyText
                      }
                      pageInfo{
                        hasNextPage
                        endCursor
                      }
                    }
                  }
                }
              }"""
              ,owner = owner, name = repo_name,number = issue_number)
              for issue_comment in issue_comments:
                issue_number2comment[issue_number].append(issue_comment['bodyText'])
          except scraper.base.VCSError:
              # repo has been deleted
              if repo_slug in repo2issue2comments:
                repo2issue2comments.pop(repo_slug)
              continue
          except:
              assert False, repo_slug
              if repo_slug in repo2issue2comments:
                repo2issue2comments.pop(repo_slug)
              continue
        
          repo2issue2comments[repo_slug] = issue_number2comment
          if repo_slug_index % save_frequency == 0:
            with open(saved_directory, 'w') as f:
              json.dump(repo2issue2comments, f)


        with open(saved_directory, 'w') as f:
          json.dump(repo2issue2comments, f)


    def get_repo2metadata(self,
                          repo_slug_list,
                          saved_directory,
                          save_frequency = 1000,
                          repo_slug2meta = {}):

        p = progressbar.ProgressBar()
        for repo_slug_index in p(range(len(repo_slug_list))):
          repo_slug = repo_slug_list[repo_slug_index]

          if repo_slug in repo_slug2meta:
            continue
          owner, repo_name = repo_slug.split('/')
          try:
            metas = self.gh_api.v4("""
            query ($owner: String!, $name:String!) {
              repository(owner:$owner, name:$name){
                  isFork
                  isInOrganization
              }
            }"""
            ,owner = owner, name = repo_name)
            repo_slug2meta[repo_slug] = {}

            for meta in metas:

                # repo_slug2meta[repo_slug]['owner_login'] = meta['repository']['owner']['login']
                repo_slug2meta[repo_slug]['isFork'] = meta['isFork']
                repo_slug2meta[repo_slug]['isInOrganization'] = meta['isInOrganization']
                break

            metas = self.gh_api.v4("""
            query ($owner: String!, $name:String!) {
              repository(owner:$owner, name:$name){
                owner {
                  login
                }
              }
            }"""
            ,owner = owner, name = repo_name)


            for meta in metas:
                if meta is not None:
                  meta = meta.lower()
                repo_slug2meta[repo_slug]['owner_login'] = meta
                break

          except scraper.base.VCSError as e:
            if repo_slug in repo_slug2meta:
              repo_slug2meta.pop(repo_slug)
            continue
          except:
            assert False, repo_slug

          if repo_slug_index % save_frequency == 0:
            with open(saved_directory, 'w') as f:
              json.dump(repo_slug2meta, f)


        with open(saved_directory, 'w') as f:
          json.dump(repo_slug2meta, f)
    def get_repo2commitlist(self, 
                            repo_slug_list,
                            saved_directory,
                            save_frequency = 10000,
                            repo_slug2commit_list = {}):

        p = progressbar.ProgressBar()
        for repo_slug_index in p(range(len(repo_slug_list))):
            repo_slug = repo_slug_list[repo_slug_index]
            if repo_slug in repo_slug2commit_list:
              continue

            owner, repo = repo_slug.split("/")

            try:
            # if True:
              commits = self.gh_api.v4("""
                      query ($owner: String!, $repo: String!, $cursor: String) {
                      repository(name: $repo, owner: $owner) {
                          defaultBranchRef{ target {
                          # object(expression: "HEAD") {
                          ... on Commit {
                              history (first: 100, after: $cursor) {
                                  nodes {sha:oid, 
                                         author{
                                          email 
                                          name
                                          user{
                                           login
                                           }
                                         }
                                         authoredDate
                                         committedDate
                                         pushedDate
                                  }
                                  pageInfo {endCursor, hasNextPage}
                      }}}}}}""", ('repository', 'defaultBranchRef', 'target', 'history'),
                                 owner=owner, repo=repo)

              repo_slug2commit_list[repo_slug] = []
              for commit in commits:
                  sha = str(commit['sha'])
                  
                  authoredDate = str(commit['authoredDate'])
                  committedDate = str(commit['committedDate'])
                  pushedDate = str(commit['pushedDate'])
                  if commit['author']['user'] is None:
                    author_login = None
                  else:
                    author_login = str(commit['author']['user']['login'])

                  author_name = commit['author']['name']
                  author_email = commit['author']['email']

                  authoredDate_converted = authoredDate.replace('T', ' ').replace('Z', '') if authoredDate is not None else None
                  committedDate_converted = committedDate.replace('T', ' ').replace('Z', '') if committedDate is not None else None
                  pushedDate_converted = pushedDate.replace('T', ' ').replace('Z', '') if pushedDate is not None else None

                  repo_slug2commit_list[repo_slug].append({"author_login":author_login, 
                                                           "author_display_name": author_name,
                                                           "author_email": author_email,
                                                           "authored_at": authoredDate_converted, 
                                                           "comitted_at": committedDate_converted,
                                                           "pushed_at": pushedDate_converted,
                                                           'sha_str': str(sha)})
            except scraper.base.VCSError:
                # repo has been deleted
                if repo_slug in repo_slug2commit_list:
                  repo_slug2commit_list.pop(repo_slug)
                continue
            except:
                assert False, repo_slug
                if repo_slug in repo_slug2commit_list:
                  repo_slug2commit_list.pop(repo_slug)
                continue
            if repo_slug_index % save_frequency == 0:
                with open(saved_directory,"w") as f:
                    json.dump(repo_slug2commit_list, f)

        with open(saved_directory,"w") as f:
            json.dump(repo_slug2commit_list, f)

    def get_repo2default_branch_commitcount(self, 
                                 repo_slug_list,
                                 datetime_start_string,
                                 datetime_end_string,
                                 saved_directory,
                                 save_frequency = 10000,
                                 repo2defaultbranch_commitcount = {}):

        p = progressbar.ProgressBar()
        for repo_index in p(range(len(repo_slug_list))):
            repo_slug = repo_slug_list[repo_index]

            assert repo_slug == repo_slug.lower(), 'repo_slug has to be lowercase'
            if repo_slug in repo2defaultbranch_commitcount:
                continue

            owner, repo_name = repo_slug.split('/')
            
            try:
              commitcounts = self.gh_api.v4("""
              query ($owner: String!, $name: String!) {
                repository(owner: $owner, name: $name) {
                  defaultBranchRef{
                    target {
                      ... on Commit {
                        
                        history(first: 0, since: "%s", until:"%s") {
                          totalCount
                        }
                      }
                    }
                  }
                }
              }"""%(datetime_start_string, datetime_end_string)
              ,('repository','defaultBranchRef'),owner = owner, name = repo_name)

              for commit_count in commitcounts:
                if commit_count is not None:
                  assert repo_slug not in repo2defaultbranch_commitcount
                  repo2defaultbranch_commitcount[repo_slug] = commit_count['target']['history']['totalCount']

            except scraper.base.VCSError as e:
              assert (valid_repository_error_constant_check in str(e))
              if repo_slug in repo2defaultbranch_commitcount:
                repo2defaultbranch_commitcount.pop(repo_slug)
              continue
            except requests.exceptions.Timeout as e:
              if repo_slug in repo2defaultbranch_commitcount:
                repo2defaultbranch_commitcount.pop(repo_slug)
              continue

            if repo_index % save_frequency == 0:
                with open(saved_directory,"w") as f:
                    json.dump(repo2defaultbranch_commitcount, f)


        with open(saved_directory,"w") as f:
            json.dump(repo2defaultbranch_commitcount, f)




    def get_repo2ref2commitcount(self, 
                                 repo_slug_list,
                                 datetime_start_string,
                                 datetime_end_string,
                                 saved_directory,
                                 save_frequency = 10000,
                                 repo2ref2commitcount = {}):

        p = progressbar.ProgressBar()
        for repo_index in p(range(len(repo_slug_list))):
            repo_slug = repo_slug_list[repo_index]

            assert repo_slug == repo_slug.lower(), 'repo_slug has to be lowercase'
            if repo_slug in repo2ref2commitcount:
                continue

            owner, repo_name = repo_slug.split('/')
            repo2ref2commitcount[repo_slug] = {}
            try:
              commitcounts = self.gh_api.v4("""
              query ($owner: String!, $name: String!,$cursor: String) {
                repository(owner: $owner, name: $name) {
                  refs(first: 100, refPrefix: "refs/heads/", after: $cursor) {
                    edges {
                      node {
                        name
                        target {
                          ... on Commit {
                            
                            history(first: 0, since: "%s", until:"%s") {
                              totalCount
                            }
                          }
                        }
                      }
                    }
                    pageInfo{
                      hasNextPage
                      endCursor
                    }
                  }
                }
              }"""%(datetime_start_string, datetime_end_string)
              ,('repository', 'refs'),owner = owner, name = repo_name)

              for commit_count in commitcounts:
                  ref_name = commit_count['node']['name']
                  total_count = commit_count['node']['target']['history']['totalCount']
                  if ref_name in repo2ref2commitcount[repo_slug]:
                    print("repo_slug",repo_slug,'ref_name',ref_name)
                    assert False
                  repo2ref2commitcount[repo_slug][ref_name] = total_count
            except scraper.base.VCSError as e:
              assert (valid_repository_error_constant_check in str(e))
              if repo_slug in repo2ref2commitcount:
                repo2ref2commitcount.pop(repo_slug)
              continue
            except requests.exceptions.Timeout as e:
              time.sleep(1)
              if repo_slug in repo2ref2commitcount:
                repo2ref2commitcount.pop(repo_slug)
              continue

            if repo_index % save_frequency == 0:
                with open(saved_directory,"w") as f:
                    json.dump(repo2ref2commitcount, f)


        with open(saved_directory,"w") as f:
            json.dump(repo2ref2commitcount, f)


    def get_user2contributionrepo2commitcount(self,
                                              login_list,
                                              datetime_start_string,
                                              datetime_end_string,
                                              saved_directory,
                                              save_frequency = 10000,
                                              login2repository2commit_count = {} # input the file of collected result
                                              ):

        p = progressbar.ProgressBar()

        for login_index in p(range(len(login_list))):
            login = login_list[login_index]
            assert login == login.lower(), 'login has to be lowercase'
            if login in login2repository2commit_count:
                continue
            login2repository2commit_count[login] = {}
            try:
              contributions = self.gh_api.v4("""
              query ($user_name: String!) {
                user(login: $user_name){
                  contributionsCollection(from:"%s", to:"%s"){
                    commitContributionsByRepository(maxRepositories:100){
                      contributions{
                        totalCount
                      }
                      repository{
                        name
                        owner {
                          login
                        }
                      }  
                    }
                  }
                }
              }"""%(datetime_start_string, datetime_end_string)
              ,('user', 'contributionsCollection','commitContributionsByRepository'),user_name = login)

              for contribution_list in contributions:
                  for contribution_item in contribution_list:
                    contribution_count = contribution_item['contributions']['totalCount']
                    repo_name = contribution_item['repository']['name']
                    repo_owner = contribution_item['repository']['owner']['login']

                    repo_slug = '/'.join([repo_owner, repo_name]).lower()
                    assert repo_slug not in login2repository2commit_count[login]
                    login2repository2commit_count[login][repo_slug] = contribution_count
            except scraper.base.VCSError as e:
              assert (valid_login_error_constant_check in str(e))
              if login in login2repository2commit_count:
                login2repository2commit_count.pop(login)
              continue
            except requests.exceptions.Timeout as e:
              if login in login2repository2commit_count:
                login2repository2commit_count.pop(login)
              continue
              
            if login_index % save_frequency == 0:
                with open(saved_directory,"w") as f:
                    json.dump(login2repository2commit_count, f)


        with open(saved_directory,"w") as f:
            json.dump(login2repository2commit_count, f)


    def get_user2follower_list(self,
                               login_list,
                               saved_directory,
                               save_frequency = 10000,
                               login2follower_list = {},
                               ):
        p = progressbar.ProgressBar()

        for login_index in p(range(len(login_list))):
            login = login_list[login_index]
            assert login == login.lower(), 'login has to be lowercase'
            if login in login2follower_list:
                continue
            login2follower_list[login] = []
            try:
              followers = self.gh_api.v4("""
              query ($user_name: String!,$cursor: String) {
                user(login: $user_name){
                  followers(first:100, after: $cursor){
                    nodes{
                      login
                    }
                    pageInfo {endCursor, hasNextPage}
                  }

                }
              }"""
              ,('user', 'followers'),user_name = login)

              for follower in followers:
                follower_login = follower['login'].lower()
                login2follower_list[login].append(follower_login)
            except scraper.base.VCSError as e:
              assert (valid_login_error_constant_check in str(e))
              if login in login2follower_list:
                login2follower_list.pop(login)
              continue
            login2follower_list[login] = list(set(login2follower_list[login]))
            if login_index % save_frequency == 0:
                with open(saved_directory,"w") as f:
                    json.dump(login2follower_list, f)


        with open(saved_directory,"w") as f:
            json.dump(login2follower_list, f)

    def get_repo2commit2additiondeletion(self,
                                         repo2commitshalist,
                                         saved_directory,
                                         save_frequency = 10000,
                                         repo2commit2info = defaultdict(dict)):

        p = progressbar.ProgressBar()
        repo_list = list(repo2commitshalist.keys())
        for repo_index in p(range(len(repo_list))):
          repo_slug = repo_list[repo_index]
          assert repo_slug == repo_slug.lower(), 'repo_slug has to be lowercase'
          owner, name = repo_slug.split('/')
          for commitsha in repo2commitshalist[repo_slug]:
            if commitsha in repo2commit2info[repo_slug]:
              continue
            try:
              commitinfos = self.gh_api.v4("""
              query ($owner: String!,$repo_name: String!,$sha:GitObjectID!) {
                repository(owner:$owner, name:$repo_name){
                  object(oid:$sha){
                    ... on Commit{
                      additions
                      deletions
                      }
                    
                  }
                }
              }"""
              ,('repository', 'object'),owner = owner, repo_name = name,sha =  commitsha)
              for commitinfo in commitinfos:
                repo2commit2info[repo_slug][commitsha] = commitinfo
                # {'additions': 10, 'deletions': 0}
                break

            except scraper.base.VCSError as e:
              continue

          if repo_index % save_frequency == 0:
              with open(saved_directory,"w") as f:
                  json.dump(repo2commit2info, f)


        with open(saved_directory,"w") as f:
            json.dump(repo2commit2info, f)

    def get_userorg_identity(self,
                            login_list,
                            saved_directory,
                            save_frequency = 1000,
                            login2identity = {}):


        p = progressbar.ProgressBar()
        for login_index in p(range(len(login_list))):
          login = login_list[login_index]
          if login in login2identity:
            continue
          assert login == login.lower(), 'Login has to be lower case'

          try:
            loginidentitys = self.gh_api.v4("""
            query ($user_login:String!) {
              repositoryOwner(login:$user_login){
                ... on User {
                  __typename
                }
                ... on Organization {
                  __typename
                }
              }
            }"""
            ,('repositoryOwner', '__typename'),user_login=login)
            for loginidentity in loginidentitys:

              if loginidentity == 'User':
                login2identity[login] = 'USR'
              elif loginidentity == 'Organization':
                login2identity[login] = 'ORG'
              else:
                assert loginidentity is None

          except scraper.base.VCSError as e:
            continue

          if login_index % save_frequency == 0:
            with open(saved_directory, 'w') as f:
              json.dump(login2identity, f)


        with open(saved_directory, 'w') as f:
          json.dump(login2identity, f)