# NES project-level GDB init file
# Lives at the repo root: nebulastream-public/.gdbinit
#
# GDB loads this automatically when you debug from the nebulastream-public
# root directory — which is what CLion does.
#
# ONE-TIME SETUP per developer (only for native debugging on the host):
#   Add this line to your ~/.gdbinit (create it if it doesn't exist):
#
#       add-auto-load-safe-path /path/to/nebulastream-public
#
#   This tells GDB it is safe to auto-load this project's .gdbinit.
#   Without it GDB silently ignores this file for security reasons.
#
#   Inside the dev container this is already set up (the image trusts the
#   mounted repo path), so no ~/.gdbinit change is needed there.

# Load the NES pretty printer. This file only auto-loads from the repo root, so
# the cwd is that root and the scripts/ path can be derived from it.
python
import os
import sys
sys.path.insert(0, os.path.join(os.getcwd(), 'scripts'))
import pretty_print_logic_plans
end

# Make STL containers readable (vectors, strings etc.)
set print pretty on

# Show all characters in strings (plans can be long)
set print elements 0

# Follow shared_ptr and virtual dispatch automatically
set print object on
