from FakeData import FakeArchiver

result = FakeArchiver()
result.gen_data(50, 10, 10)
result.import_data("txt.txt")
result.update_data("Hold.|Night.|Season.|Firm.|Last deep.|Firm life.")
result.save_as_file("xlsx", "xlsx")
result.get_archive("arc", "zip", 100000)
