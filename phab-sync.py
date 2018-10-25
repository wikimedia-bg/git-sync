#!/usr/bin/env python3

import json
import os
import os.path
import re
import signal
import sys
import time

from datetime import datetime as dt
from datetime import timedelta as td
from pathlib import Path

import git
import pywikibot as pwb


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

    def sleep(self, seconds):
        if self._exit_requested:
            self._exit_now()
        else:
            self._is_sleeping = True
            time.sleep(seconds)
            self._is_sleeping = False


class LogPage(pwb.Page):

    def __init__(self, source, title):
        super().__init__(source, title)

    def log(self, log_type, user, repo_name, commit_sha,
            datetime, message, target_page, oldid=None):
        page_link = {
                'edit': '| {{{{diff|prev|{oldid}|{page}}}}} |',
                'delete': '| {{{{del|{page}}}}} |',
                'conflict': '| [[{page}]] |'
                }
        self.text = self.text.replace(
                '{{/Header}}',
                '{{{{/Header}}}}\n'
                '| {{{{/{log_type}}}}} |'.format(log_type=log_type)
                + page_link[log_type].format(oldid=oldid, page=target_page) +
                '| {{{{потребител|{user}}}}} |'
                '| {{{{ph|source/{repo}/commit/{sha}|{datetime}}}}} |'
                '| {message}\n|-'.format(
                    user=user,
                    repo=repo_name,
                    sha=commit_sha,
                    datetime=datetime,
                    message=message.replace('\n', '')))
        try:
            self.save(summary='Регистриране на ново действие в дневника.',
                      botflag=True, quiet=True)
        except pwb.data.api.APIError as e:
            print('APIError exception: {}'.format(str(e)), file=sys.stderr)


class PhabRepo:

    def __init__(self, name, repo, site, log_page, namespace, title_regex, force_ext, ignores):
        self.name = name
        self.repo = repo
        self.site = site
        self.log_page = log_page
        self.namespace = namespace
        self.title_regex = title_regex
        self.force_ext = force_ext
        if force_ext:
            self.re_force_ext = re.compile(r'\.' + force_ext + '$')
        self.ignores = ignores
        self._pending_commits = {}

    def _pagelist(self):
        return [_ for _ in self.site.allpages(namespace=self.namespace)
                if self.title_regex.search(_.title(with_ns=False))]

    def _last_changed(self):
        return dt.utcfromtimestamp(self.repo.commit('master').authored_date) + td(seconds=1)

    def _pending_revs(self, resync):
        revs = []
        for page in self._pagelist():
            pending_revs = page.revisions(endtime=self._last_changed(), content=True)
            revs += [(page.title(with_ns=False), _, 'edit') for _ in pending_revs]
            if resync:
                # Full re-sync requested, so get the latest revision of _all_ pages in the repo.
                last_rev = page.latest_revision
                revs.append((page.title(with_ns=False), {
                    'user': 'syncbot',
                    'comment': 'forced resync from wiki',
                    'text': last_rev['text'],
                    'timestamp': dt.utcnow(),
                    }, 'resync'))
        # We need to also check for deleted pages that we keep track of.
        repo_files = [_.path for _ in self.repo.tree().traverse() if _.type != 'tree']
        page_files = set(repo_files) - set(self.ignores)
        repo_pages = [self.re_force_ext('', _.replace('.d/', '/')) for _ in page_files]
        existing_pages = [_.title(with_ns=False) for _ in self._pagelist()]
        deleted_pages = set(repo_pages) - set(existing_pages)
        for page_name in deleted_pages:
            for event in self.site.logevents(page=self.namespace + ':' + page_name):
                if event.type() in ['delete', 'move']:
                    revs.append((page_name, {
                        'user': event.user(),
                        'comment': event.comment(),
                        'timestamp': event.timestamp(),
                        }, event.type()))
        revs.sort(key=lambda rev: rev[1]['timestamp'])
        return revs

    def _pull(self):
        old_master = self.repo.commit('master')
        self.repo.git.pull()
        new_master = self.repo.commit('master')
        if new_master == old_master:
            return
        pull_commits_newest_first = self.repo.iter_commits(
                old_master.hexsha + '...' + self.repo.commit('master').hexsha)
        pull_commits = reversed([_ for _ in pull_commits_newest_first])
        # This requires Python 3.7+ to keep the insertion order of the dictionary.
        for commit in pull_commits:
            self._pending_commits[commit] = self.repo.git.diff_tree(
                    '--no-commit-id', '--name-only', '-r',
                    commit.parents[0], commit
                    ).split('\n')

    def _wiki2phab(self, resync):
        revs = self._pending_revs(resync)
        synced_files = []
        for rev in revs:
            # Ignore our sync edits in the wiki.
            if rev[1]['user'] == self.site.username():
                continue
            # Iliev is the only user to have a username on Phabricator that's not the same as the
            # MediaWiki username, so let Phabricator perform the committer matching by email.
            elif rev[1]['user'] == 'Iliev':
                user_mail = 'luchesar.iliev@gmail.com'
            else:
                user_mail = ''
            if not rev[1]['comment']:
                comment = '*** празно резюме ***'
            else:
                comment = rev[1]['comment']
            # We cannot have both a file and a directory with the same name, so where we have
            # 'Page' and 'Page/doc', the latter gets converted to 'Page.d/doc'.
            file_name = rev[0].replace('/', '.d/')
            # If we've configured a file extension for syntax highlighting, add it, but only for
            # files in the root of the namespace/repository (the rest will likely be 'Page/doc').
            if self.force_ext and '.d/' not in file_name:
                file_name = file_name + '.' + self.force_ext
            file_path = os.path.join(self.repo.working_dir, file_name)
            # To avoid conflicts as much as possible, perform git pull right before we apply the
            # change and commit it.
            self._pull()
            if rev[2] in ['edit', 'resync']:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                if rev[2] == 'resync' and os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        if rev[1]['text'] == f.read().rstrip('\n'):
                            # The on-wiki and Phabricator versions are the same. No need to resync.
                            continue
                with open(file_path, 'w') as f:
                    f.write(rev[1]['text'] + '\n')
                self.repo.index.add([file_path])
            elif rev[2] in ['delete', 'move']:
                self.repo.index.remove([file_path], working_tree=True)
            else:
                print('Error: Unknown revision type: "{}"'.format(rev[2]))
                continue
            author = git.Actor(rev[1]['user'].replace(' ', '_'), user_mail)
            committer = git.Actor(rev[1]['user'].replace(' ', '_'), user_mail)
            print('Syncing to Phabricator: {}'.format(file_name))
            self.repo.index.commit(
                    comment,
                    author=author,
                    committer=committer,
                    author_date=str(rev[1]['timestamp'])[:-1],
                    commit_date=str(rev[1]['timestamp'])[:-1])
            # Push after each commit. It's inefficient, but should minimize possible conflicts.
            self.repo.git.push()
            synced_files.append(file_name)
        return synced_files

    def _phab2wiki(self, synced_from_wiki):
        # Iterate over a list of the keys, instead of directly on the dictionary. This allows to
        # delete the pending commits from the latter once they are processed.
        commit_list = list(self._pending_commits)
        for commit in commit_list:
            if re.search(r'\bDO\s+NOT\s+(MERGE|SYNC)\b', commit.message):
                print('Ignoring commit {} because of DO NOT MERGE/SYNC.'.format(commit.hexsha))
                del self._pending_commits[commit]
                continue
            for file_name in self._pending_commits[commit]:
                # We cannot have both a file and a directory with the same name, so where we have
                # 'Page' and 'Page/doc', the latter was converted to 'Page.d/doc'.
                page_name = self.namespace + ':' + file_name.replace('.d/', '/')
                # If we've configured a file extension for syntax highlighting, remove it, but only
                # for files in the root of the namespace/repo (the rest will likely be 'Page/doc').
                if self.force_ext and '/' not in page_name:
                    page_name = self.re_force_ext.sub('', page_name)
                page = pwb.Page(self.site, page_name)
                committer = re.sub(r'\s<>$', '', commit.committer.name)
                if file_name in synced_from_wiki:
                    # This page has been updated on the wiki in this sync run. To be on the safe
                    # side, we'll discard the possibly conflicting changes from Phabricator.
                    print('Ignoring possibly conflicting changes in {}'.format(file_name))
                    self.log_page.log(
                            log_type='conflict',
                            user=committer,
                            repo_name=self.name,
                            commit_sha=commit.hexsha,
                            datetime=commit.committed_datetime,
                            message=commit.message,
                            target_page=page.title())
                    continue
                file_removed = False
                try:
                    file_git_blob = self.repo.commit(commit).tree.join(file_name)
                except KeyError as e:
                    if str(e).endswith('\'{file}\' not found"'.format(file=file_name)):
                        file_removed = True
                    else:
                        print('WARNING: Unexpected KeyError exception in _phab2wiki().')
                if not file_removed:
                    file_contents_at_commit = b''.join(file_git_blob.data_stream[3].readlines())
                    page.text = file_contents_at_commit.decode('utf-8').rstrip('\n')
                    print('Saving {}'.format(page.title()))
                    try:
                        page.save(
                                summary='[[User:{user}|{user}]] @ {datetime}: {message}'.format(
                                    user=committer,
                                    datetime=commit.committed_datetime,
                                    message=commit.message.replace('\n', '')),
                                botflag=True, quiet=True)
                    except pwb.data.api.APIError as e:
                        print('APIError exception: {}'.format(str(e)), file=sys.stderr)
                    else:
                        self.log_page.log(
                                log_type='edit',
                                user=committer,
                                repo_name=self.name,
                                commit_sha=commit.hexsha,
                                datetime=commit.committed_datetime,
                                message=commit.message,
                                target_page=page.title(),
                                oldid=page.latest_revision_id)
                # if file_removed is True.
                else:
                    print('Deleting {}'.format(page.title()))
                    try:
                        page.delete(
                                reason='[[User:{user}|{user}]] @ {datetime}: {message}'.format(
                                    user=committer,
                                    datetime=commit.committed_datetime,
                                    message=commit.message.replace('\n', '')),
                                prompt=False)
                    except pwb.data.api.APIError as e:
                        print('APIError exception: {}'.format(str(e)), file=sys.stderr)
                    else:
                        self.log_page.log(
                                log_type='delete',
                                user=committer,
                                repo_name=self.name,
                                commit_sha=commit.hexsha,
                                datetime=commit.committed_datetime,
                                message=commit.message,
                                target_page=page.title())
            # When all files in a commit have been processed, remove it from the pending list.
            del self._pending_commits[commit]

    def sync(self, resync=False):
        w2ph_synced_files = self._wiki2phab(resync)
        self._pull()
        if self._pending_commits:
            self._phab2wiki(w2ph_synced_files)


def init_repos(config):
    repos = []
    for repo in config['repos']:
        file_regex = re.compile(
                repo['file_regex'],
                re.I if repo['regex_nocase'] else 0)
        repo_path = os.path.join(config['repositories_root'], repo['name'])
        git_repo = git.Repo(repo_path)
        site = pwb.Site(
                code=repo['project']['code'],
                fam=repo['project']['family'],
                user=config['mediawiki_username'],
                sysop=config['mediawiki_username'])
        log_page = LogPage(site, repo['log_page'])
        repos.append(PhabRepo(repo['name'], git_repo, site, log_page,
                              repo['namespace'], file_regex, repo['force_extension'],
                              repo['ignore_list'] + config['global_ignore_list']))
    return repos


def read_config():
    config_file_name = 'phab-sync.config.json'
    config_files = [
            config_file_name,
            os.path.join(Path.home(), '.config/phab-sync', config_file_name),
            os.path.join('/etc/phab-sync', config_file_name),
            ]
    for config_file in config_files:
        if os.path.exists(config_file):
            with open(config_file, 'rb') as f:
                config = json.load(f)
            break
    try:
        return(config)
    except UnboundLocalError:
        print('Error: Configuration file not found.', file=sys.stderr)
        sys.exit(1)


def main(argv):
    sig_handler = SignalHandler()
    config = read_config()
    repos = init_repos(config)
    if argv:
        if argv.pop() in ['resync', 'force']:
            for repo in repos:
                print('Resyncing repo "{}"...'.format(repo.repo.git_dir))
                repo.sync(resync=True)
    while True:
        for repo in repos:
            print('Syncing repo "{}"...'.format(repo.repo.git_dir))
            repo.sync()
        print('Sleeping...')
        sig_handler.sleep(config['daemon_sleep_seconds'])


if __name__ == '__main__':
    main(sys.argv[1:])

# vim: set ts=4 sts=4 sw=4 et:
