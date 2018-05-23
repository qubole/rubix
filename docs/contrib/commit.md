# Commit Message

Commits are used as a source of truth for various reports. A couple of examples are:

* Release Notes
* Issues resolved for QA to plan the QA cycle.

To be able to generate these reports, uniform commit messages are required. 
All your commits should follow the following convention:

For every commit please write a short (max 72 characters) summary in the first
line followed with a blank line and then more detailed descriptions of the 
change.

Format of summary:

    ACTION: AUDIENCE: COMMIT_MSG

Description:

    ACTION is one of 'chg', 'fix', 'new'
    Is WHAT the change is about.
    'chg' is for refactor, small improvement, cosmetic changes...
    'fix' is for bug fixes
    'new' is for new features, big improvement

    AUDIENCE is one of 'dev', 'usr', 'pkg', 'test', 'doc'
    Is WHO is concerned by the change.
    'dev' is for developers (API changes, refactors...)
    'usr' is for final users

You will use your environment’s default editor (EDITOR=vi|emacs) to compose the
commit message. Do NOT use the command line git commit -m “my mesg” as this 
only allows you to write a single line that most of the times turns out to be 
useless to others reading or reviewing your commit.

## Example

    new: dev: #124: report liveness metric for BookKeeper daemon (#139)

    Add a liveness gauge that the daemon is up & alive. Right now, this 
    is a simple check that a thread (reporter to be added in a subsequent 
    commit) is alive. In the future, this simple framework will be used 
    to add more comprehensive health checks. Ref: #140
    
The above example shows the commit summary is:

* a single line composed of four columns
* column 1 tells us the nature of the change or ACTION:  new
* a short one-line summary of WHAT the commit is doing

The description or the body of the commit message delves into more detail 
that is intended to serve as a history for developers on the team on how the
code is evolving. There are more immediate uses of this description however. 
When you raise pull requests to make your contributions into the project, your 
commit descriptions serve as explanations of WHY you fixed an issue. HOW you 
fixed an issue is explained by code already. This is also the place where the 
peer-reviewers will begin understanding your code. An unclear commit message is 
the source of a lot of back and forth resulting in frustration between 
reviewers and committers.
 
Reference: <http://chris.beams.io/posts/git-commit/>

