#!/usr/bin/env python3

from datetime import datetime as dt
from datetime import timedelta as td
import os
import os.path
import re
import signal
import sys
import time

import git
import pywikibot as pwb


BOT_USERNAME = 'PSS 9'
SLEEP_SEC = 10


class SignalHandler:

    def __init__(self):
        self._is_sleeping = False
        self._exit_requested = False
        signal.signal(signal.SIGINT, self._request_exit)
        signal.signal(signal.SIGTERM, self._request_exit)

    def _request_exit(self, signal, frame):
        if self._is_sleeping:
            self._exit_now()
        else:
            self._exit_requested = True

    def _exit_now(self):
        print('SIGINT or SIGTERM received, exiting...')
        sys.exit(0)

    def sleep(self, seconds=SLEEP_SEC):
        if self._exit_requested:
            self._exit_now()
        else:
            self._is_sleeping = True
            time.sleep(seconds)
            self._is_sleeping = False


class PhabRepo:

    def __init__(self, repo, site, ns, t_re):
        self.repo = repo
        self.site = site
        self.ns = ns
        self.t_re = t_re
        self._pending_files = []

    def _allpages(self):
        return self.site.allpages(namespace=self.ns)

    def _last_changed(self):
        return dt.utcfromtimestamp(self.repo.commit('master').authored_date) + td(seconds=1)

    def _revlist(self):
        revs = []
        for page in self._allpages():
            page_name = page.title(with_ns=False)
            if self.t_re.search(page_name):
                for rev in page.revisions(endtime=self._last_changed(), content=True):
                    revs.append((page_name, rev))
        revs.sort(key=lambda rev: rev[1]['timestamp'])
        return revs

    def _pull(self):
        old_master = self.repo.commit('master')
        self.repo.git.pull()
        # Obtain all changed files in this pull.
        self._pending_files += self.repo.git.diff_tree(
                '--no-commit-id',
                '--name-only',
                '-r',
                old_master,
                self.repo.commit('master')).split('\n')

    def _wiki2phab(self):
        revs = self._revlist()
        for rev in revs:
            if rev[1]['user'] == BOT_USERNAME:
                continue
            elif rev[1]['user'] == 'Iliev':
                user_mail = 'luchesar.iliev@gmail.com'
            else:
                user_mail = ''
            if not rev[1]['comment']:
                comment = '*** празно резюме ***'
            else:
                comment = rev[1]['comment']
            file_name = rev[0].replace('/', '.d/')
            file_path = os.path.join(self.repo.working_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # To avoid conflicts as much as possible, perform git pull right before we apply the
            # change and commit it.
            self._pull()
            with open(file_path, 'w') as f:
                f.write(rev[1]['text'])
            self.repo.index.add([file_path])
            author = git.Actor(rev[1]['user'].replace(' ', '_'), user_mail)
            committer = git.Actor(rev[1]['user'].replace(' ', '_'), user_mail)
            self.repo.index.commit(
                    comment,
                    author=author,
                    committer=committer,
                    author_date=str(rev[1]['timestamp'])[:-1],
                    commit_date=str(rev[1]['timestamp'])[:-1])
            # Push after each commit. It's inefficient, but should minimize possible conflicts.
            self.repo.git.push()
        # Return the list of files that have been synced from Wikipedia.
        return [_[0].replace('/', '.d/') for _ in revs]

    def _phab2wiki(self, files_tosync):
        for file_name in files_tosync:
            file_path = os.path.join(self.repo.working_dir, file_name)
            page = pwb.Page(self.site, self.ns + ':' + file_name.replace('.d/', '/'))
            with open(file_path, 'r') as f:
                page.text = f.read()
            page.save('Sync from Phabricator', botflag=True)

    def sync(self):
        w2ph_synced_files = self._wiki2phab()
        self._pull()
        # If no files have been changed in Phabricator, self._pending_files is set to [''] (list
        # with a single element, which itself is an empty string). This is _not_ an empty list.
        ph2w_pending_files = list(filter(None, self._pending_files))
        # Determine which pages need to be updated on Wikipedia. To be on the safe side, if during
        # one sync operation a page is changed both on Wikipedia and on Phabricator, the Wikipedia
        # version wins. We do this by simply substracting the list of files that were updated on
        # Wikipedia from the list of files updated on Phabricator (converting to a set() first).
        ph2w_files_tosync = set(ph2w_pending_files) - set(w2ph_synced_files)
        if ph2w_files_tosync:
            self._phab2wiki(ph2w_files_tosync)
            self._pending_files = []


def sync_repos(repos):
    for repo in repos:
        repo.sync()


def main(argv):
    sig_handler = SignalHandler()

    rpath_base = '/home/kerb/.local/share/phab-sync/repos'
    rname_spam = 'spam'
    rname_tbl = 'tbl'
    rname_lua = 'lua'
    rname_ui = 'ui'

    rpath_spam = os.path.join(rpath_base, rname_spam)
    rpath_tbl = os.path.join(rpath_base, rname_tbl)
    rpath_lua = os.path.join(rpath_base, rname_lua)
    rpath_ui = os.path.join(rpath_base, rname_ui)

    repo_spam = git.Repo(rpath_spam)
    repo_tbl = git.Repo(rpath_tbl)
    repo_lua = git.Repo(rpath_lua)
    repo_ui = git.Repo(rpath_ui)

    re_spam = re.compile(r'^Spam')
    re_tbl = re.compile(r'^Title')
    re_lua = re.compile(r'.*')
    re_ui = re.compile(r'(^gadgets?-|\.(css|js)\b)', re.I)

    site = pwb.Site()

    r_spam = PhabRepo(repo_spam, site, 'MediaWiki', re_spam)
    r_tbl = PhabRepo(repo_tbl, site, 'MediaWiki', re_tbl)
    r_lua = PhabRepo(repo_lua, site, 'Module', re_lua)
    r_ui = PhabRepo(repo_ui, site, 'MediaWiki', re_ui)

    repos = [
            r_spam,
            r_tbl,
            r_lua,
            r_ui,
            ]

    while True:
        print('Running...')
        sync_repos(repos)
        print('Sleeping...')
        sig_handler.sleep()


if __name__ == '__main__':
    main(sys.argv[1:])

# vim: set ts=4 sts=4 sw=4 et:
