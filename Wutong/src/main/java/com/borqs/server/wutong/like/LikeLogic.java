package com.borqs.server.wutong.like;


import com.borqs.server.base.context.Context;
import com.borqs.server.base.data.Record;
import com.borqs.server.base.data.RecordSet;

import java.util.List;

public interface LikeLogic {
    boolean createLike(Context ctx,String userId, String targetId);
    boolean destroyLike(Context ctx,String userId, String targetId);
    boolean ifUserLiked(Context ctx,String userId, String targetId);

    int getLikeCount(Context ctx,String targetId);
    RecordSet loadLikedUsers(Context ctx,String targetId, int page, int count);
    RecordSet getLikedPost(Context ctx,String userId, int page, int count,int objectType) ;
    RecordSet getObjectLikedByUsers(Context ctx,String viewerId,String userIds,String objectType, int page, int count) ;
    boolean updateLikeTarget(Context ctx,String old_target, String new_target);

    boolean likeP(Context ctx,String userId, int objectType, String target, String device, String location, String appId) ;
    boolean unlikeP(Context ctx, String userId, int objectType, String target);
    int getLikeCountP(Context ctx,int objectType, String target);
    RecordSet likedUsersP(Context ctx,String userId, int objectType, String target, String cols, int page, int count);
    boolean ifuserLikedP(Context ctx,String userId, String targetId);
    RecordSet getLikedPostsP(Context ctx,String userId, String cols, int objectType, int page, int count);
}