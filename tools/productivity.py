import json
from datetime import datetime, timedelta
from termcolor import cprint
import random
import time
import sys

def load_tasks():
    try:
        with open('/Users/bobbyyo/Projects/algo-fun/tools/tasks.json', 'r') as f:
            tasks = json.load(f)
        return tasks
    except FileNotFoundError:
        cprint("Error: tasks.json file not found!", 'white', 'on_red')
        sys.exit(1)
    except json.JSONDecodeError:
        cprint("Error: Invalid JSON in tasks.json!", 'white', 'on_red')
        sys.exit(1)

def get_tasks_schedule(tasks):
    task_start_time = datetime.now()
    schedule = []
    for task, minutes in tasks.items():
        end_time = task_start_time + timedelta(minutes=minutes)
        schedule.append((task, task_start_time, end_time))
        task_start_time = end_time
    return schedule 

def main():
    tasks = load_tasks()
    if not tasks:
        cprint("Error: No tasks found in tasks.json!", 'white', 'on_red')
        sys.exit(1)
        
    schedule = get_tasks_schedule(tasks)
    current_index = 0

    while True:
        now = datetime.now()
        
        # Check if we've completed all tasks
        if current_index >= len(schedule):
            cprint("🎉 All tasks completed! 🎉", 'white', 'on_green')
            break
            
        current_task, start_time, end_time = schedule[current_index]
        remaining_time = end_time - now
        remaining_minutes = int(remaining_time.total_seconds() // 60)

        print('')

        for index, (task, s_time, e_time) in enumerate(schedule):
            if index < current_index:
                # task is completed
                print(f"Task {task} done {e_time.strftime('%H:%M')}")
            elif index == current_index:
                # current task is running
                if remaining_minutes < 2:
                    cprint(f'{task} < 2m left!', 'white', 'on_red', attrs=['blink'])
                elif remaining_minutes < 5:
                    cprint(f'{task} - {remaining_minutes} mins', 'white', 'on_red')
                else:
                    cprint(f'{task} - {remaining_minutes} mins', 'white', 'on_blue')
            else:
                print(f"{task} @ {s_time.strftime('%H:%M')}")

        list_of_reminders = [
            "I have a 1000 percent chance of success",
            "I am a genius",
            "I am a hard worker",
            "I am a good person",
            "I am a good programmer",
            "I am a good trader",
            "I am a good investor",
            "I am a good person",
            "Everyday I get better and better",
            "Rest at the END",
            "Jobs Not Finished!",
            "I am the best algo trader in the world",
        ]

        random_reminder = random.choice(list_of_reminders)
        print('💫 ' + random_reminder)

        if now >= end_time:
            current_index += 1

        time.sleep(15)

if __name__ == "__main__":
    main()