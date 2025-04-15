#!/bin/bash

ssh-keyscan github.com >> .ssh/known_hosts
bin/chezmoi init git@github.com:vrdel/dotfiles
bin/chezmoi apply
rm -rf .local/share/chezmoi
