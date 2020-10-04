# This file is used to autoload presets in this folder when a feed URL from
# certain websites is provided but the PRESET option is not set.
#
# Deleting this file disables this feature, and deleting/renaming predefined
# presets in this folder causes auto-load for that website to be disabled.

_SITES = {
    r'.*\.livejournal\.com/?.*': 'livejournal',
    r'.*\.tumblr\.com/?.*': 'tumblr',
    r'.*\.wordpress\.com/?.*': 'wordpress',
}
