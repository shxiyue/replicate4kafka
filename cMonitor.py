#-*- coding: UTF-8 -*-
import curses
import time


class monitor():
    def __init__(self):
        self.stdscr = curses.initscr()
        self.set_win()

    def __del__(self):
        self.unset_win()

    def display_info(self,str, x, y, colorpair=2):
        '''''使用指定的colorpair显示文字'''  
        self.stdscr.addstr(y, x,str, curses.color_pair(colorpair))
        self.stdscr.refresh()

    def get_ch_and_continue(self):
        '''''演示press any key to continue'''
        #设置nodelay，为0时会变成阻塞式等待
        self.stdscr.nodelay(0)
        #输入一个字符
        ch=self.stdscr.getch()
        #重置nodelay,使得控制台可以以非阻塞的方式接受控制台输入，超时1秒
        self.stdscr.nodelay(1)
        return True

    def set_win(self):
        '''''控制台设置'''
        #使用颜色首先需要调用这个方法
        curses.start_color()
        #文字和背景色设置，设置了两个color pair，分别为1和2
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
        #关闭屏幕回显
        curses.noecho()
        #输入时不需要回车确认
        curses.cbreak()
        #设置nodelay，使得控制台可以以非阻塞的方式接受控制台输入，超时1秒
        self.stdscr.nodelay(1)

    def unset_win(self):
        '''控制台重置'''
        #恢复控制台默认设置（若不恢复，会导致即使程序结束退出了，控制台仍然是没有回显的）
        curses.nocbreak()
        self.stdscr.keypad(0)
        curses.echo()
        #结束窗口
        curses.endwin()

if __name__=='__main__':
    m = monitor()
    try:
        for i in range(10):
            m.display_info('Hola, curses!%d'%(i),0,5)
            m.display_info('seconde !%d'%(i),0,6)
            time.sleep(0.5)
        #m.display_info('Press any key to continue...',0,10)
        #m.get_ch_and_continue()
    except Exception,e:
        raise e
