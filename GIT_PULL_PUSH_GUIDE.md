## Hướng dẫn `git pull` / `git push`

Tài liệu này hướng dẫn thao tác cơ bản với Git: cập nhật code về máy (`pull`) và đẩy code lên remote (`push`).

### 0) Kiểm tra trạng thái hiện tại

```powershell
git status
```

Nếu có file đang sửa/chưa commit, hãy commit trước khi `push` (xem mục 2).

### 1) `pull` để lấy thay đổi mới nhất từ remote

#### Trường hợp bình thường (branch `main`)

```powershell
git pull origin main
```

Nếu repo bạn cần history gọn hơn, có thể dùng:

```powershell
git pull --rebase origin main
```

#### Kiểm tra remote/branch đang dùng

```powershell
git remote -v
git branch --show-current
```

### 2) Commit (nếu bạn có thay đổi cục bộ)

```powershell
git add -A
git commit -m "your message"
```

### 3) `push` để đẩy thay đổi lên remote

#### Lần đầu trên branch `main` (hoặc chưa set upstream)

```powershell
git push -u origin main
```

#### Các lần sau

```powershell
git push
```

### 4) Xử lý xung đột (conflict)

Nếu `pull` hoặc `push` báo conflict:
1. `git status` để xem file nào bị conflict.
2. Mở file đó, sửa thủ công cho đúng.
3. Commit sau khi đã sửa:
   ```powershell
   git add -A
   git commit -m "fix conflicts"
   ```
4. Sau đó `push` lại:
   ```powershell
   git push
   ```

### 5) Mẹo nhanh

```powershell
git fetch
git status
```

`fetch` chỉ tải dữ liệu mới về (không merge). Khi đã sẵn sàng thì `pull` để merge/rebase.

