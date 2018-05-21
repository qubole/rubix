# How to contribute code on Github

### 1. Create a branch and start working on your change.
  

    cd rubix
    git checkout -b new_rubix_branch
     
### 2. Code
   
* Adhere to code standards.
* Include tests and ensure they pass.

### 3. Commit

For every commit please write a short (max 72 characters) summary in the first
line followed with a blank line and then more detailed descriptions of the 
change.

_Don't forget a prefix!_

More details in [Commit Guidelines](https://rubix.readthedocs.io/en/latest/contrib/commit.html)

### 4. Update your branch
  
  
    git fetch upstream
    git rebase upstream/master
  
### 5. Push to remote


    git push -u origin new_rubix_branch
   
### 6. Issue a [Pull Request](https://help.github.com/articles/proposing-changes-to-a-project-with-pull-requests/)

* Navigate to the Rubix repository you just pushed to (e.g. <https://github.com/your-user-name/rubix>)
* Click _Pull Request_.
* Write your branch name in the branch field (this is filled with _master_ by default)
* Click _Update Commit Range_.
* Ensure the changesets you introduced are included in the _Commits_ tab.
* Ensure that the _Files Changed_ incorporate all of your changes.
* Fill in some details about your potential patch including a meaningful title. 
* Click _Send pull request_.

### 7. Respond to feedback

The RubiX team may recommend adjustments to your code. Part of interacting with
a healthy open-source community requires you to be open to learning new 
techniques and strategies; _don’t get discouraged!_ Remember: if the RubiX 
team suggest changes to your code, they care enough about your work that they 
want to include it, and hope that you can assist by implementing those 
revisions on your own.

### 8. Postscript

Once all the changes are approved, one contributor will push the change to the upstream code.
