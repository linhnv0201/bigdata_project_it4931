import os
import subprocess

def upload_to_hdfs(local_dir, hdfs_dir):
    """
    Kiểm tra và tải toàn bộ file từ thư mục local vào HDFS nếu chưa tồn tại.

    Args:
        local_dir (str): Đường dẫn thư mục local chứa file.
        hdfs_dir (str): Thư mục HDFS để upload dữ liệu.
    """
    # Tạo thư mục trên HDFS nếu chưa tồn tại
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)

    # Lặp qua tất cả các file trong thư mục local
    for file_name in os.listdir(local_dir):
        local_file_path = os.path.join(local_dir, file_name)
        hdfs_file_path = os.path.join(hdfs_dir, file_name)

        # Kiểm tra file tồn tại trong HDFS
        check_command = ["hdfs", "dfs", "-test", "-e", hdfs_file_path]
        exists = subprocess.run(check_command, capture_output=True)

        if exists.returncode == 0:
            print(f"File {file_name} đã tồn tại trên HDFS, bỏ qua...")
        else:
            print(f"Đang upload {file_name} vào HDFS...")
            upload_command = ["hdfs", "dfs", "-put", local_file_path, hdfs_file_path]
            subprocess.run(upload_command, check=True)

if __name__ == "__main__":
    # Thư mục local chứa các file
    local_data_dir = "./data"

    # Thư mục HDFS đích
    hdfs_target_dir = "/user/hdfs/batch_data"

    # Upload dữ liệu
    upload_to_hdfs(local_data_dir, hdfs_target_dir)
