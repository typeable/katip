module Katip.Scribes.RotatingHandler where

import           Control.Concurrent.STM
import           Control.DeepSeq
import           Data.ByteString.Lazy   as BL

-- | File owner struct writing data to the file
-- sequentially. Automatic buffer flushing and log rotating.
data FileOwner = FileOwner
  { foDataQueune :: TBQueue BL.ByteString

  }

data ControlMsg
  = CloseMsg
  | FlushMsg
  | ReopenMsg

newFileOwner :: FilePath -> Maybe Int -> IO FileOwner
newFileOwner = error "newFileOwner not implemented"

fileOwnerControl :: FileOwner -> ControlMsg -> IO ()
fileOwnerControl = error "fileOwnerControl not implemented"

writeFileOwner :: FileOwner -> BL.ByteString -> IO ()
writeFileOwner fo bs = do
  a <- return $!! bs
  atomically $ writeTBQueue (foDataQueune fo) $!! a
