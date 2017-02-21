`FishDB` is a key-value storage based on b-tree model.

## Install
```c++
make all
```

## API
Instance operations:
```c++
BTree *bt = BTre::Open(dbfile);
...
bt->Close();
delete bt;
```
Key-value operations:
```c++
bt->Put('key', 'value');
```
Iterating over key space:
```c++
auto iter = bt->NewIterator();
for (iter->SeekToFirst(); iter->Valid(); iter->Next())
{
	std::cout << iter->Key() << ": " << iter->Value() << std::endl;
}
delete iter;
```
