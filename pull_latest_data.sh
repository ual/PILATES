#!/bin/bash

#Modified from github user MisaOgura
#https://github.com/MisaOgura/checkout-and-pull-master/blob/master/checkout_and_pull.sh

yellow=$(tput setaf 3)
blue=$(tput setaf 4)
reset=$(tput sgr0)

for i in $(find pilates/beam/production -maxdepth 1 -mindepth 1 -type d); do
    if [ -d $i/.git ]; then
        dirname=$(basename "$i")
        repo="pilates/beam/production/$dirname/.git"
        worktree="$PWD/pilates/beam/production/$dirname"
        echo "----------"
        echo "${blue}$dirname${reset}"
        echo "${yellow}Step 1 of 2: checking out to pilates data branch...${reset}"
        git --git-dir=$repo --work-tree=$worktree checkout pilates -f
        git --git-dir=$repo --work-tree=$worktree reset --hard HEAD
        echo "${yellow}Step 2 of 2: pulling from pilates...${reset}"
        git --git-dir=$repo --work-tree=$worktree pull
    fi
done