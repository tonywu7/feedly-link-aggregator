# MIT License
#
# Copyright (c) 2020 Tony Wu <tony[dot]wu(at)nyu[dot]edu>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from cmd import Cmd

from scrapy.cmdline import execute
from scrapy.commands import ScrapyCommand

from ..cli import cli
from ..datastructures import labeled_sequence
from .utils import (_disable_initial_log, _LoggingMixin, _restore_sigint,
                    dir_validator, exists_validator)


class WizardCommand(ScrapyCommand, _LoggingMixin):
    requires_project = True

    def short_desc(self):
        return 'Easy-to-use wizard for common tasks'

    def add_options(self, parser):
        return super().add_options(parser)

    def process_options(self, args, opts):
        super().process_options(args, opts)
        self._takeover_logging()
        _disable_initial_log()

    def run(self, args, opts):
        _restore_sigint()
        Wizard().cmdloop()


class Wizard(Cmd):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intro = '\nWhat would you like to do today?\nPlease type in one of the above commands'
        self.prompt = '> '
        self.initial = True

    def _run_program(self, args, executor):
        print('\nCommand: ')
        print(' '.join(args))
        print()

        try:
            executor(args)
        except SystemExit as e:
            exitcode = e.code
        finally:
            _restore_sigint()

        if exitcode:
            print(f'Subprocess exited with code {exitcode}')
            print('An error occured.')
            return 0

    def _should_export(self, output):
        should_export = Form(Form.yes_or_no('yes', 'Would you like to export scraped data?'))
        should_export.cmdloop()
        if should_export.formdata.get('yes'):
            self.do_export(output)

    def _fill_form(self, form):
        form.cmdloop()
        if not form.filled:
            print('Task cancelled.')
            return
        return True

    def do_scrape(self, arg=None):
        """Scrape an RSS feed."""
        print(self.do_scrape.__doc__)

        form = Form(
            ['rss', 'Please enter the URL to the site you would like to crawl', None, None],
            ['output', 'Please enter a path to a folder where scraped data will be saved', None, dir_validator],
        )
        if not self._fill_form(form):
            return

        args = ['python3', '-m', 'scrapy', 'crawl', 'feed',
                '-s', f'RSS={form.formdata["rss"]}',
                '-s', f'OUTPUT={form.formdata["output"]}']

        exitcode = self._run_program(args[2:], execute)
        if exitcode == 0:
            return exitcode

        print(f'\nFinished scraping {form.formdata["rss"]}')
        self._should_export(form.formdata['output'])
        print('Task finished.')
        return 0

    def do_resume(self, arg=None):
        """Resume an unfinished scraping task."""
        print(self.do_resume.__doc__)

        form = Form(
            ['output', 'Please enter the path to the output folder of a previously ran task', None, dir_validator],
        )
        if not self._fill_form(form):
            return

        args = ['python3', '-m', 'scrapy', 'resume', form.formdata['output']]
        exitcode = self._run_program(args[2:], execute)
        if exitcode == 0:
            return exitcode

        print('Task finished.')
        return 0

    def do_export(self, arg=None):
        """Extract URLs from scraped data."""
        print(self.do_export.__doc__)
        arg = None if not arg else arg

        form = Form(
            ['output', 'Please enter the path to the scraped data', arg, exists_validator],
            Form.multiplechoice(
                'type', ['all', 'images', 'audio/video', 'clickable links'],
                'What type of URLs would you like to export?', 1,
            ),
            Form.multiplechoice(
                'fmt', [
                    'do not categorize',
                    'by the websites from which they are found',
                    'by the websites that they point to',
                ], 'Would you like to categorize the exported URLs?', 1),
        )
        if not self._fill_form(form):
            return

        ftr = {
            1: [],
            2: ['+f', 'tag', 'is', 'img'],
            3: ['+f', 'tag', 'is', 'object'],
            4: ['+f', 'tag', 'is', 'a'],
        }
        fmt = {
            1: [],
            2: ['-o', '%(source:netloc)s.txt'],
            3: ['-o', '%(target:netloc)s.txt'],
        }

        args = ['python3', '-m', 'scrapy', 'export',
                'urls', '-i', form.formdata['output'],
                *ftr[form.formdata['type']],
                *fmt[form.formdata['fmt']]]
        self._run_program(args[3:], cli)

    def do_quit(self, arg=None):
        """Close this wizard."""
        print('Goodbye!')
        return 0

    def do_help(self, arg=None):
        """Show this help."""
        names = self.get_names()
        docs = {}
        pad = 0
        for n in names:
            if n[:3] == 'do_':
                cmd = n[3:]
                docs[cmd] = getattr(self, n).__doc__
                if len(cmd) + 3 > pad:
                    pad = len(cmd) + 3

        if arg:
            if arg not in docs:
                print(f'No such command "{arg}"!')
            else:
                print(docs[arg])
            return

        print('Things you can do:\n')
        for k, v in sorted(docs.items()):
            k = k.ljust(pad)
            print(f'  {k} - {v}')

    def default(self, line):
        print(f'No such command `{line}`!')

    def emptyline(self):
        return self.do_help()

    def postcmd(self, stop, line):
        if stop == 0:
            return True
        if self.initial:
            self.prompt = '\nWhat else would you like to do?\n> '
            self.initial = False
        return super().postcmd(stop, line)

    def cmdloop(self, intro=None):
        self.do_help()
        try:
            return super().cmdloop(intro)
        except KeyboardInterrupt:
            print('Aborted!')
            quit(1)


class Form(Cmd):
    def __init__(self, *questions):
        super().__init__(None)
        self._form = {}
        self._question = iter(questions)
        self.filled = False
        self._advance()

    @property
    def formdata(self):
        return self._form

    def _advance(self):
        try:
            self._current = next(self._question)
        except StopIteration:
            return 0
        else:
            _, prompt, default, _ = self._current
            if default is None:
                self.prompt = f'\n{prompt}\n>>> '
            else:
                self.prompt = f'\n{prompt}\n[{default}] '

    def postcmd(self, should_advance, line):
        if not should_advance:
            return
        stop = self._advance()
        if stop == 0:
            self.filled = True
            return True

    def emptyline(self):
        key, _, default, _ = self._current
        if default is None:
            print('Please enter a value.')
        else:
            return self.default(default)

    def default(self, line):
        key, _, _, validator = self._current
        validator = validator or (lambda v: v)
        try:
            value = validator(line)
        except Exception as e:
            print(f'Error: {e}')
        else:
            self._form[key] = value
            return True

    def do_help(self, arg):
        pass

    def cmdloop(self, intro=None):
        try:
            return super().cmdloop(intro)
        except KeyboardInterrupt:
            pass

    @classmethod
    def yes_or_no(cls, key, prompt='Confirm?', default='yes', strict=False):
        return [key, f'{prompt} (yes or no)', default, cls._yestobool(strict)]

    @classmethod
    def multiplechoice(cls, key, choices, prompt='Choose one of the following', default=None):
        numbered_choices = labeled_sequence(choices, start=1, key=False)
        choices = [f'  {v}. {k}' for k, v in numbered_choices.items()]
        choices = '\n'.join(choices)
        prompt = f'{prompt}\n{choices}\n(Enter a number)'
        return [key, prompt, default, cls._selectchoice(numbered_choices)]

    @staticmethod
    def _selectchoice(choices):
        def validator(t):
            try:
                t = int(t)
            except ValueError:
                pass
            if isinstance(t, int):
                if t not in set(choices.values()):
                    raise ValueError('Invalid option.')
                return t
            if t not in choices:
                raise ValueError('Invalid option.')
            return choices[t]
        return validator

    @staticmethod
    def _yestobool(strict=False):
        if strict:
            def validator(t):
                if t == 'yes':
                    return True
                if t == 'no':
                    return False
                raise ValueError('Must be "yes" or "no".')
            return validator
        else:
            def validator(t):
                if t[0].lower() == 'y':
                    return True
                return False
            return validator
