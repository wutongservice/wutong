
package com.borqs.server.wutong.tag;


import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;

public interface TagLogic  {
    Record createTag(Context ctx,Record tag);

    boolean destroyedTag(Context ctx,Record tag);

    boolean hasTag(Context ctx,String userId, String tag);

    boolean hasTarget(Context ctx,String userId, String target_id, String type);

    RecordSet findUserByTag(Context ctx,String tag, int page, int count) ;

    RecordSet findTagByUser(Context ctx,String user_id, int page, int count) ;

    RecordSet findTargetsByTag(Context ctx,String tag, String type, int page, int count);

    RecordSet findTargetsByUser(Context ctx,String userId, String type, int page, int count);

    RecordSet findUserTagByTarget(Context ctx,String target_id, String type, int page, int count);

}