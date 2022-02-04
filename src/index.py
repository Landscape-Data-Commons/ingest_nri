import os, os.path, sys
from cmd import Cmd
import logging

class main(Cmd):

    def __init__(self):
        super(main, self).__init__()
        self.prompt = "> "

    def do_test(self,args):
      print("test")

    def do_exit(self, args):
        raise SystemExit()

if __name__=="__main__":
    app = main()
    app.cmdloop("Currently available commands: test, exit help.")
