import os, os.path, sys
from cmd import Cmd
import logging

import interface as inter

class main(Cmd):



    def __init__(self):
        super(main, self).__init__()
        self.prompt = "> "
        self.ingestpath = os.path.normpath(os.path.join(os.getcwd(),'/ingestables/nri_update'))
        self.accesspath = os.path.dirname(self.ingestpath)

    def do_ingest(self, mbd:bool):

      print(self.ingestpath,self.accesspath, mbd)
      # inter.batcher(self.ingestpath, se;f.accesspath, mbd)

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: ingest, exit, and help.")
